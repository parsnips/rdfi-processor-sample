package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	defaultGraphQLURL     = "http://localhost:8080/financial/v1/graphql"
	defaultWebhookURL     = "http://host.docker.internal:8099/rdfi"
	defaultListenAddr     = "0.0.0.0:8099"
	defaultTwispAccountID = "000000000000"

	endUserAccountID    = "6c6affb0-5cf5-402b-8d84-01bfc1624a2c"
	pendingAccountID    = "9fd08f4c-c740-4f9b-89fa-9e0536b326e5"
	settlementAccountID = "0689377a-2683-11f0-9999-069b540ea27b"
	suspenseAccountID   = "167d713c-2683-11f0-9c43-069b540ea27b"
	exceptionAccountID  = "5d6743ea-0da2-4698-a66d-80cd1e172a7e"
	feeAccountID        = "46866383-8914-4058-a64f-5c79cafef1d4"
)

type scenario struct {
	ID                string
	Name              string
	Description       string
	Template          string
	DDA               string
	OnCreate          hookResponse
	OnSettle          hookResponse
	ExpectedReturn    bool
	ExpectedNOC       bool
	ManualReturn      bool
	RetryOnCreate     bool
	AutoPending       bool
	ExpectedException bool
	ReturnCode        string
	ReturnInfo        string
}

type hookRequest struct {
	ExecutionID   string         `json:"executionId"`
	WorkflowTask  string         `json:"workflowTask"`
	WorkflowName  string         `json:"workflowName"`
	FileKey       string         `json:"fileKey"`
	AccountID     string         `json:"accountId"`
	FileHeader    map[string]any `json:"fileHeader"`
	BatchHeader   map[string]any `json:"batchHeader"`
	EntryDetail   map[string]any `json:"entryDetail"`
	Addenda99     map[string]any `json:"addenda99"`
	HookType      string         `json:"hookType"`
	Metadata      any            `json:"metadata"`
	EntryMetadata any            `json:"entryMetadata"`
}

type hookResponse struct {
	Action        string         `json:"action,omitempty"`
	AccountID     string         `json:"accountId,omitempty"`
	When          string         `json:"when,omitempty"`
	Addenda99     map[string]any `json:"addenda99,omitempty"`
	Addenda98     map[string]any `json:"addenda98,omitempty"`
	Metadata      any            `json:"metadata,omitempty"`
	EntryMetadata any            `json:"entryMetadata,omitempty"`
}

type webhookEvent struct {
	ScenarioID string       `json:"scenarioId"`
	Request    hookRequest  `json:"request"`
	Response   hookResponse `json:"response"`
	At         time.Time    `json:"at"`
}

type webhookServer struct {
	byDDA          map[string]*scenario
	mu             sync.Mutex
	events         []webhookEvent
	createAttempts map[string]int
}

func (s *webhookServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req hookRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	dda := stringField(req.EntryDetail, "dfiAccountNumber", "dfi_account_number")
	sc := s.byDDA[strings.TrimSpace(dda)]
	resp := hookResponse{}
	if sc == nil {
		resp = hookResponse{
			Action:    "RETURN",
			Addenda99: addenda99("R03", "No account mapped for DDA "+strings.TrimSpace(dda)),
		}
	} else {
		switch strings.ToUpper(req.WorkflowTask) {
		case "CREATE":
			s.mu.Lock()
			s.createAttempts[sc.ID]++
			attempt := s.createAttempts[sc.ID]
			s.mu.Unlock()
			if sc.RetryOnCreate && attempt == 1 {
				resp = hookResponse{Action: "RETRY", AccountID: endUserAccountID}
			} else {
				resp = sc.OnCreate
			}
		case "SETTLE":
			resp = sc.OnSettle
		default:
			resp = hookResponse{}
		}
	}

	s.mu.Lock()
	event := webhookEvent{Request: req, Response: resp, At: time.Now()}
	if sc != nil {
		event.ScenarioID = sc.ID
	}
	s.events = append(s.events, event)
	s.mu.Unlock()

	fmt.Println("\n--- webhook request ---")
	printJSON(json.RawMessage(body))
	fmt.Println("--- webhook response ---")
	printJSON(resp)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *webhookServer) eventsFor(id string) []webhookEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []webhookEvent
	for _, event := range s.events {
		if event.ScenarioID == id {
			out = append(out, event)
		}
	}
	return out
}

func (s *webhookServer) executionFor(id string) (hookRequest, bool) {
	events := s.eventsFor(id)
	for _, event := range events {
		if strings.EqualFold(event.Request.WorkflowTask, "CREATE") {
			return event.Request, true
		}
	}
	return hookRequest{}, false
}

type appConfig struct {
	GraphQLURL     string
	WebhookURL     string
	ListenAddr     string
	TwispAccountID string
	Authorization  string
	Pattern        string
	Timeout        time.Duration
}

type runner struct {
	cfg                 appConfig
	client              *gqlClient
	runID               string
	endpoint            string
	configID            string
	autoPendingConfigID string
	webhooks            *webhookServer
	scenarios           []*scenario
}

func main() {
	var cfg appConfig
	flag.StringVar(&cfg.GraphQLURL, "graphql", envOr("TWISP_GRAPHQL_URL", defaultGraphQLURL), "Twisp GraphQL endpoint")
	flag.StringVar(&cfg.WebhookURL, "webhook-url", envOr("RDFI_WEBHOOK_URL", defaultWebhookURL), "URL Twisp should call for RDFI webhooks")
	flag.StringVar(&cfg.ListenAddr, "listen", envOr("RDFI_LISTEN_ADDR", defaultListenAddr), "local webhook listener address")
	flag.StringVar(&cfg.TwispAccountID, "twisp-account-id", envOr("TWISP_ACCOUNT_ID", defaultTwispAccountID), "x-twisp-account-id header value")
	flag.StringVar(&cfg.Authorization, "authorization", os.Getenv("AUTHORIZATION"), "optional Authorization header value")
	flag.StringVar(&cfg.Pattern, "scenario", "", "scenario id/name regexp; empty runs all scenarios serially")
	flag.DurationVar(&cfg.Timeout, "timeout", 90*time.Second, "per-scenario timeout")
	flag.Parse()

	ctx := context.Background()
	all := scenarios()
	selected, err := selectScenarios(all, cfg.Pattern)
	if err != nil {
		log.Fatal(err)
	}

	byDDA := make(map[string]*scenario)
	for _, sc := range all {
		byDDA[sc.DDA] = sc
	}
	webhooks := &webhookServer{byDDA: byDDA, createAttempts: make(map[string]int)}
	server := &http.Server{Addr: cfg.ListenAddr, Handler: webhooks}

	ln, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		log.Fatalf("listen on %s: %v", cfg.ListenAddr, err)
	}
	go func() {
		if err := server.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("webhook server stopped: %v", err)
		}
	}()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	r := &runner{
		cfg:                 cfg,
		client:              newGQLClient(cfg),
		runID:               shortID(),
		endpoint:            newUUID(),
		configID:            newUUID(),
		autoPendingConfigID: newUUID(),
		webhooks:            webhooks,
		scenarios:           selected,
	}

	fmt.Printf("RDFI webhook listening on http://%s/rdfi\n", cfg.ListenAddr)
	fmt.Printf("Twisp webhook URL configured as %s\n", cfg.WebhookURL)
	fmt.Printf("Run id: %s\n", r.runID)

	if err := r.setup(ctx); err != nil {
		log.Fatal(err)
	}

	for i, sc := range selected {
		fmt.Printf("\n[%d/%d] %s\n%s\n", i+1, len(selected), sc.Name, sc.Description)
		if err := r.runScenario(ctx, sc); err != nil {
			log.Fatalf("scenario %s failed: %v", sc.ID, err)
		}
	}

	fmt.Println("\nAll selected scenarios completed.")
}

func (r *runner) setup(ctx context.Context) error {
	fmt.Println("\n== setup ==")
	if err := r.createAccounts(ctx); err != nil {
		return err
	}
	if err := r.createEndpoint(ctx); err != nil {
		return err
	}
	if err := r.createConfiguration(ctx); err != nil {
		return err
	}
	if err := r.createAutoPendingConfiguration(ctx); err != nil {
		return err
	}
	return nil
}

func (r *runner) runScenario(ctx context.Context, sc *scenario) error {
	ctx, cancel := context.WithTimeout(ctx, r.cfg.Timeout)
	defer cancel()

	fileKey := fmt.Sprintf("rdfi-example-%s-%s.ach", r.runID, sc.ID)
	returnKey := fmt.Sprintf("rdfi-example-%s-%s-return.ach", r.runID, sc.ID)
	nocKey := fmt.Sprintf("rdfi-example-%s-%s-noc.ach", r.runID, sc.ID)
	file := makeACHFile(sc.Template, sc.DDA)
	configID := r.configID
	if sc.AutoPending {
		configID = r.autoPendingConfigID
	}

	upload, err := r.createUpload(ctx, fileKey)
	if err != nil {
		return err
	}
	if err := putFile(ctx, upload.UploadURL, upload.Headers, []byte(file)); err != nil {
		return err
	}

	if err := r.processFile(ctx, configID, fileKey); err != nil {
		return err
	}
	if sc.AutoPending {
		return r.finishAutoPendingScenario(ctx, sc, configID, fileKey)
	}
	if _, err := r.waitForFile(ctx, configID, fileKey, "PROCESSED", "COMPLETED"); err != nil {
		return err
	}

	if sc.ManualReturn {
		req, ok := r.webhooks.executionFor(sc.ID)
		if !ok {
			return fmt.Errorf("manual pending scenario did not receive CREATE webhook")
		}
		printManualWorkflow(req)
		if err := r.executeWorkflow(ctx, req, "RETURN", sc.ReturnCode, sc.ReturnInfo); err != nil {
			return err
		}
	}

	if sc.ExpectedReturn || sc.ManualReturn {
		if err := r.generateAndRender(ctx, "RDFI_RETURN", returnKey); err != nil {
			return err
		}
	}
	if sc.ExpectedNOC {
		if err := r.generateAndRender(ctx, "RDFI_NOC", nocKey); err != nil {
			return err
		}
	}

	return nil
}

func (r *runner) createAccounts(ctx context.Context) error {
	query := `
mutation SetupAccounts($endUser: UUID!, $pending: UUID!, $settlement: UUID!, $suspense: UUID!, $exception: UUID!, $fee: UUID!) {
  endUser: createAccount(input: {
    accountId: $endUser, code: "rdfi-example.end-user", name: "RDFI Example End User",
    config: { idempotent: true }
  }) { accountId }
  pending: createAccount(input: {
    accountId: $pending, code: "rdfi-example.pending", name: "RDFI Example Auto-Pending",
    config: { enableConcurrentPosting: true, idempotent: true }
  }) { accountId }
  settlement: createAccount(input: {
    accountId: $settlement, code: "rdfi-example.settlement", name: "RDFI Example Settlement",
    normalBalanceType: DEBIT, config: { enableConcurrentPosting: true, idempotent: true }
  }) { accountId }
  suspense: createAccount(input: {
    accountId: $suspense, code: "rdfi-example.suspense", name: "RDFI Example Suspense",
    config: { enableConcurrentPosting: true, idempotent: true }
  }) { accountId }
  exception: createAccount(input: {
    accountId: $exception, code: "rdfi-example.exception", name: "RDFI Example Exception",
    config: { enableConcurrentPosting: true, idempotent: true }
  }) { accountId }
  fee: createAccount(input: {
    accountId: $fee, code: "rdfi-example.fee", name: "RDFI Example Fee",
    config: { enableConcurrentPosting: true, idempotent: true }
  }) { accountId }
}`
	var resp map[string]any
	return r.client.do(ctx, "create accounts", query, map[string]any{
		"endUser":    endUserAccountID,
		"pending":    pendingAccountID,
		"settlement": settlementAccountID,
		"suspense":   suspenseAccountID,
		"exception":  exceptionAccountID,
		"fee":        feeAccountID,
	}, &resp)
}

func (r *runner) createAutoPendingConfiguration(ctx context.Context) error {
	query := `
mutation CreateAutoPendingConfiguration($configId: UUID!, $settlement: UUID!, $pending: UUID!, $suspense: UUID!, $exception: UUID!) {
  ach {
    createConfiguration(input: {
      configId: $configId
      direction: RDFI
      autoPending: true
      pendingAccountId: $pending
      settlementAccountId: $settlement
      exceptionAccountId: $exception
      suspenseAccountId: $suspense
      journalId: "00000000-0000-0000-0000-000000000000"
      odfiHeaderConfiguration: {
        immediateDestination: "026009593"
        immediateDestinationName: "ACME BANK"
        immediateOrigin: "111000173"
        immediateOriginName: "RDFI EXAMPLE"
      }
      timeZone: "America/Los_Angeles"
    }) {
      configId
      direction
      autoPending
      pendingAccountId
    }
  }
}`
	var resp map[string]any
	return r.client.do(ctx, "create auto-pending configuration", query, map[string]any{
		"configId":   r.autoPendingConfigID,
		"settlement": settlementAccountID,
		"pending":    pendingAccountID,
		"suspense":   suspenseAccountID,
		"exception":  exceptionAccountID,
	}, &resp)
}

func (r *runner) createEndpoint(ctx context.Context) error {
	query := `
mutation CreateEndpoint($endpointId: UUID!, $url: String!) {
  events {
    createEndpoint(input: {
      endpointId: $endpointId
      status: ENABLED
      endpointType: ACH_PROCESSOR
      url: $url
      subscription: []
      description: "RDFI example processor"
    }) { endpointId url }
  }
}`
	var resp map[string]any
	return r.client.do(ctx, "create endpoint", query, map[string]any{
		"endpointId": r.endpoint,
		"url":        r.cfg.WebhookURL,
	}, &resp)
}

func (r *runner) createConfiguration(ctx context.Context) error {
	query := `
mutation CreateConfiguration($configId: UUID!, $endpointId: UUID!, $settlement: UUID!, $suspense: UUID!, $exception: UUID!, $fee: UUID!) {
  ach {
    createConfiguration(input: {
      configId: $configId
      endpointId: $endpointId
      settlementAccountId: $settlement
      exceptionAccountId: $exception
      suspenseAccountId: $suspense
      feeAccountId: $fee
      journalId: "00000000-0000-0000-0000-000000000000"
      odfiHeaderConfiguration: {
        immediateDestination: "026009593"
        immediateDestinationName: "ACME BANK"
        immediateOrigin: "111000173"
        immediateOriginName: "RDFI EXAMPLE"
      }
      timeZone: "America/Los_Angeles"
    }) { configId }
  }
}`
	var resp map[string]any
	return r.client.do(ctx, "create configuration", query, map[string]any{
		"configId":   r.configID,
		"endpointId": r.endpoint,
		"settlement": settlementAccountID,
		"suspense":   suspenseAccountID,
		"exception":  exceptionAccountID,
		"fee":        feeAccountID,
	}, &resp)
}

type uploadResponse struct {
	UploadURL string
	Headers   map[string]string
}

func (r *runner) createUpload(ctx context.Context, key string) (*uploadResponse, error) {
	query := `
mutation CreateUpload($key: String!) {
  files {
    createUpload(input: { key: $key, uploadType: ACH, contentType: "text/plain" }) {
      key
      uploadURL
      contentType
    }
  }
}`
	var resp struct {
		Files struct {
			CreateUpload struct {
				Key           string         `json:"key"`
				UploadURL     string         `json:"uploadURL"`
				UploadHeaders map[string]any `json:"uploadHeaders"`
			} `json:"createUpload"`
		} `json:"files"`
	}
	if err := r.client.do(ctx, "create upload", query, map[string]any{"key": key}, &resp); err != nil {
		return nil, err
	}
	fmt.Println("create upload response:")
	printJSON(resp.Files.CreateUpload)
	return &uploadResponse{UploadURL: resp.Files.CreateUpload.UploadURL, Headers: stringMap(resp.Files.CreateUpload.UploadHeaders)}, nil
}

func (r *runner) processFile(ctx context.Context, configID, key string) error {
	query := `
mutation ProcessFile($configId: UUID!, $fileKey: String!) {
  ach {
    processFile(input: { fileType: RDFI, fileKey: $fileKey, configId: $configId }) {
      fileId
    }
  }
}`
	var resp map[string]any
	if err := r.client.do(ctx, "process file", query, map[string]any{"configId": configID, "fileKey": key}, &resp); err != nil {
		return err
	}
	fmt.Println("process file response:")
	printJSON(resp)
	return nil
}

type fileInfo struct {
	FileID           string `json:"fileId"`
	ProcessingStatus string `json:"processingStatus"`
	ProcessingDetail string `json:"processingDetail"`
	HasExceptions    bool   `json:"hasExceptions"`
}

func (r *runner) waitForFile(ctx context.Context, configID, key string, statuses ...string) (*fileInfo, error) {
	query := `
query FileInfo($configId: UUID!, $fileKey: String!) {
  ach {
    file(configId: $configId, fileKey: $fileKey) {
      fileId
      processingStatus
      processingDetail
      hasExceptions
    }
  }
}`
	for {
		var resp struct {
			Ach struct {
				File *fileInfo `json:"file"`
			} `json:"ach"`
		}
		if err := r.client.do(ctx, "read file status", query, map[string]any{"configId": configID, "fileKey": key}, &resp); err != nil {
			return nil, err
		}
		if resp.Ach.File != nil {
			switch resp.Ach.File.ProcessingStatus {
			case "ERROR", "INVALID", "ABORTED":
				printJSON(resp.Ach.File)
				return nil, fmt.Errorf("file status %s: %s", resp.Ach.File.ProcessingStatus, resp.Ach.File.ProcessingDetail)
			}
			for _, status := range statuses {
				if resp.Ach.File.ProcessingStatus == status {
					fmt.Println("file status:")
					printJSON(resp.Ach.File)
					return resp.Ach.File, nil
				}
			}
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(750 * time.Millisecond):
		}
	}
}

type pendedExecution struct {
	WorkflowID  string         `json:"workflowId"`
	ExecutionID string         `json:"executionId"`
	Task        string         `json:"task"`
	Context     map[string]any `json:"context"`
}

func (r *runner) finishAutoPendingScenario(ctx context.Context, sc *scenario, configID, fileKey string) error {
	if sc.ExpectedException {
		info, err := r.waitForFile(ctx, configID, fileKey, "PENDING", "COMPLETED")
		if err != nil {
			return err
		}
		if !info.HasExceptions {
			return fmt.Errorf("unmatched return file did not set hasExceptions")
		}
		fmt.Printf("unmatched return posted to exception account %s; hasExceptions=true\n", exceptionAccountID)
		return r.printAutoPendingBalances(ctx, "after unmatched return")
	}

	if _, err := r.waitForFile(ctx, configID, fileKey, "PENDING"); err != nil {
		return err
	}
	executions, err := r.pendedExecutions(ctx, configID, fileKey)
	if err != nil {
		return err
	}
	if len(executions) == 0 {
		return fmt.Errorf("auto-pending file has no PENDING workflow executions")
	}

	if err := r.printAutoPendingBalances(ctx, "after automatic PENDING posting"); err != nil {
		return err
	}
	for _, execution := range executions {
		if execution.Task != "PENDING" {
			return fmt.Errorf("execution %s task = %s, want PENDING", execution.ExecutionID, execution.Task)
		}
		accountID := stringField(execution.Context, "accountId")
		if accountID != pendingAccountID {
			return fmt.Errorf("execution %s accountId = %s, want pending account %s", execution.ExecutionID, accountID, pendingAccountID)
		}

		moved, err := r.executeTask(ctx, execution.WorkflowID, execution.ExecutionID, "PENDING", map[string]any{
			"accountId": endUserAccountID,
		})
		if err != nil {
			return err
		}
		if accountID := stringField(moved.Context, "accountId"); accountID != endUserAccountID {
			return fmt.Errorf("moved execution %s accountId = %s, want end-user account %s", execution.ExecutionID, accountID, endUserAccountID)
		}
	}
	if err := r.printAutoPendingBalances(ctx, "after moving PENDING entries to the end-user account"); err != nil {
		return err
	}

	for _, execution := range executions {
		settled, err := r.executeTask(ctx, execution.WorkflowID, execution.ExecutionID, "SETTLE", map[string]any{
			"effective": time.Now().Format("2006-01-02"),
		})
		if err != nil {
			return err
		}
		if settled.Task != "SETTLE" {
			return fmt.Errorf("execution %s task = %s, want SETTLE", execution.ExecutionID, settled.Task)
		}
		if accountID := stringField(settled.Context, "accountId"); accountID != endUserAccountID {
			return fmt.Errorf("settled execution %s accountId = %s, want end-user account %s", execution.ExecutionID, accountID, endUserAccountID)
		}
	}
	if err := r.printAutoPendingBalances(ctx, "after SETTLE into the end-user account"); err != nil {
		return err
	}
	fmt.Println("all auto-pended entries were moved to the end-user account and settled; the file monitor will transition PENDING to COMPLETED on its next poll")
	return nil
}

func (r *runner) pendedExecutions(ctx context.Context, configID, fileKey string) ([]pendedExecution, error) {
	query := `
query PendedEntries($configId: UUID!, $fileKey: String!) {
  ach {
    file(configId: $configId, fileKey: $fileKey) {
      records(first: 1000) {
        nodes {
          recordType
          execution {
            workflowId
            executionId
            task
            context
          }
        }
      }
    }
  }
}`
	var resp struct {
		Ach struct {
			File *struct {
				Records struct {
					Nodes []struct {
						RecordType string           `json:"recordType"`
						Execution  *pendedExecution `json:"execution"`
					} `json:"nodes"`
				} `json:"records"`
			} `json:"file"`
		} `json:"ach"`
	}
	if err := r.client.do(ctx, "discover auto-pended entries", query, map[string]any{
		"configId": configID,
		"fileKey":  fileKey,
	}, &resp); err != nil {
		return nil, err
	}
	if resp.Ach.File == nil {
		return nil, fmt.Errorf("file %s not found", fileKey)
	}
	var executions []pendedExecution
	for _, node := range resp.Ach.File.Records.Nodes {
		if node.Execution != nil {
			executions = append(executions, *node.Execution)
		}
	}
	return executions, nil
}

func (r *runner) executeTask(ctx context.Context, workflowID, executionID, task string, params map[string]any) (*pendedExecution, error) {
	query := `
mutation ExecuteAutoPendingTask($workflowId: UUID!, $executionId: UUID!, $task: String!, $params: JSON) {
  workflow {
    executeTask(input: { workflowId: $workflowId, executionId: $executionId, task: $task, params: $params }) {
      workflowId
      executionId
      task
      context
      output { state }
      error
    }
  }
}`
	var resp struct {
		Workflow struct {
			ExecuteTask pendedExecution `json:"executeTask"`
		} `json:"workflow"`
	}
	if err := r.client.do(ctx, "execute auto-pending "+task, query, map[string]any{
		"workflowId":  workflowID,
		"executionId": executionID,
		"task":        task,
		"params":      params,
	}, &resp); err != nil {
		return nil, err
	}
	fmt.Printf("auto-pending %s response:\n", task)
	printJSON(resp.Workflow.ExecuteTask)
	return &resp.Workflow.ExecuteTask, nil
}

func (r *runner) printAutoPendingBalances(ctx context.Context, label string) error {
	query := `
query AutoPendingBalances($pending: UUID!, $endUser: UUID!, $exception: UUID!) {
  pending: balance(accountId: $pending, journalId: "00000000-0000-0000-0000-000000000000", currency: "USD", materialize: true) {
    settled { drBalance { units currency } crBalance { units currency } }
    encumbrance { drBalance { units currency } crBalance { units currency } }
  }
  endUser: balance(accountId: $endUser, journalId: "00000000-0000-0000-0000-000000000000", currency: "USD", materialize: true) {
    settled { drBalance { units currency } crBalance { units currency } }
    encumbrance { drBalance { units currency } crBalance { units currency } }
  }
  exception: balance(accountId: $exception, journalId: "00000000-0000-0000-0000-000000000000", currency: "USD", materialize: true) {
    settled { drBalance { units currency } crBalance { units currency } }
    encumbrance { drBalance { units currency } crBalance { units currency } }
  }
}`
	var resp map[string]any
	if err := r.client.do(ctx, "read balances "+label, query, map[string]any{
		"pending":   pendingAccountID,
		"endUser":   endUserAccountID,
		"exception": exceptionAccountID,
	}, &resp); err != nil {
		return err
	}
	fmt.Printf("balances %s:\n", label)
	printJSON(resp)
	return nil
}

func (r *runner) executeWorkflow(ctx context.Context, req hookRequest, task, returnCode, returnInfo string) error {
	code := "ACH_RDFI_DR"
	if strings.Contains(req.WorkflowName, "CR") {
		code = "ACH_RDFI_CR"
	}
	query := `
mutation ExecuteWorkflow($executionId: UUID!, $code: String!, $task: String!, $params: JSON) {
  workflow {
    execute(input: { executionId: $executionId, code: $code, task: $task, params: $params }) {
      executionId
      task
      output { state }
      error
    }
  }
}`
	params := map[string]any{
		"effective": time.Now().Format("2006-01-02"),
		"addenda99": map[string]any{
			"returnCode":         returnCode,
			"addendaInformation": returnInfo,
		},
	}
	var resp map[string]any
	if err := r.client.do(ctx, "manual workflow "+task, query, map[string]any{
		"executionId": req.ExecutionID,
		"code":        code,
		"task":        task,
		"params":      params,
	}, &resp); err != nil {
		return err
	}
	fmt.Println("manual workflow response:")
	printJSON(resp)
	return nil
}

func (r *runner) generateAndRender(ctx context.Context, fileType, key string) error {
	query := `
mutation GenerateFile($configId: UUID!, $fileKey: String!, $fileType: AchFileType!) {
  ach {
    generateFile(input: { configId: $configId, fileKey: $fileKey, fileType: $fileType, generateEmpty: false }) {
      fileKey
      generated
    }
  }
}`
	var resp struct {
		Ach struct {
			GenerateFile struct {
				FileKey   string `json:"fileKey"`
				Generated bool   `json:"generated"`
			} `json:"generateFile"`
		} `json:"ach"`
	}
	if err := r.client.do(ctx, "generate "+fileType, query, map[string]any{
		"configId": r.configID,
		"fileKey":  key,
		"fileType": fileType,
	}, &resp); err != nil {
		return err
	}
	fmt.Printf("generate %s response:\n", fileType)
	printJSON(resp.Ach.GenerateFile)
	if !resp.Ach.GenerateFile.Generated {
		return nil
	}
	return r.downloadAndPrint(ctx, key)
}

func (r *runner) downloadAndPrint(ctx context.Context, key string) error {
	query := `
mutation CreateDownload($key: String!) {
  files {
    createDownload(key: $key) {
      key
      downloadURL
      contentType
    }
  }
}`
	var resp struct {
		Files struct {
			CreateDownload struct {
				Key             string         `json:"key"`
				DownloadURL     string         `json:"downloadURL"`
				DownloadHeaders map[string]any `json:"downloadHeaders"`
				ContentType     string         `json:"contentType"`
			} `json:"createDownload"`
		} `json:"files"`
	}
	if err := r.client.do(ctx, "create download", query, map[string]any{"key": key}, &resp); err != nil {
		return err
	}
	fmt.Println("download response:")
	printJSON(resp.Files.CreateDownload)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, resp.Files.CreateDownload.DownloadURL, nil)
	if err != nil {
		return err
	}
	for k, v := range stringMap(resp.Files.CreateDownload.DownloadHeaders) {
		req.Header.Set(k, v)
	}
	httpResp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()
	body, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return err
	}
	if httpResp.StatusCode >= 300 {
		return fmt.Errorf("download %s: status %d: %s", key, httpResp.StatusCode, string(body))
	}
	fmt.Printf("rendered %s:\n%s\n", key, string(body))
	return nil
}

type gqlClient struct {
	cfg appConfig
	hc  *http.Client
}

type gqlRequest struct {
	Query     string         `json:"query"`
	Variables map[string]any `json:"variables,omitempty"`
}

type gqlResponse struct {
	Data   json.RawMessage `json:"data"`
	Errors []struct {
		Message    string         `json:"message"`
		Path       []any          `json:"path"`
		Extensions map[string]any `json:"extensions"`
	} `json:"errors"`
}

func newGQLClient(cfg appConfig) *gqlClient {
	return &gqlClient{cfg: cfg, hc: &http.Client{Timeout: 30 * time.Second}}
}

func (c *gqlClient) do(ctx context.Context, label, query string, variables map[string]any, out any) error {
	body, err := json.Marshal(gqlRequest{Query: query, Variables: variables})
	if err != nil {
		return err
	}
	fmt.Printf("\n--- graphql %s request ---\n", label)
	printJSON(json.RawMessage(body))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.GraphQLURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Twisp-Account-Id", c.cfg.TwispAccountID)
	req.Header.Set("x-twisp-internal-64e6e735255b", "true")
	if c.cfg.Authorization != "" {
		req.Header.Set("Authorization", c.cfg.Authorization)
	}

	httpResp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()
	respBody, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return err
	}
	if httpResp.StatusCode >= 300 {
		return fmt.Errorf("%s: HTTP %d: %s", label, httpResp.StatusCode, string(respBody))
	}

	var resp gqlResponse
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return fmt.Errorf("%s: decode graphql response: %w: %s", label, err, string(respBody))
	}
	if len(resp.Errors) > 0 {
		b, _ := json.MarshalIndent(resp.Errors, "", "  ")
		return fmt.Errorf("%s: graphql errors: %s", label, string(b))
	}
	if out != nil {
		if err := json.Unmarshal(resp.Data, out); err != nil {
			return fmt.Errorf("%s: decode data: %w: %s", label, err, string(resp.Data))
		}
	}
	fmt.Printf("--- graphql %s response ---\n", label)
	printJSON(json.RawMessage(resp.Data))
	return nil
}

func putFile(ctx context.Context, url string, headers map[string]string, body []byte) error {
	fmt.Println("upload request:")
	fmt.Printf("PUT %s (%d bytes)\n", url, len(body))
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "text/plain")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	fmt.Printf("upload response: HTTP %d\n", resp.StatusCode)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("upload failed: HTTP %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

func scenarios() []*scenario {
	return []*scenario{
		{
			ID:          "autopend-ppd-debit-settle",
			Name:        "Auto-pended PPD debit settled to an end user",
			Description: "An endpoint-free RDFI configuration automatically pends the debit on its pending account. The demo moves that hold to the end-user account with PENDING, then executes SETTLE.",
			Template:    ppdDebitACH,
			DDA:         "200000001",
			AutoPending: true,
		},
		{
			ID:          "autopend-ppd-credit-settle",
			Name:        "Auto-pended PPD credit settled to an end user",
			Description: "An endpoint-free RDFI configuration automatically pends the credit, moves it from the shared pending account to the end-user account, and settles it there.",
			Template:    ppdCreditACH,
			DDA:         "200000002",
			AutoPending: true,
		},
		{
			ID:                "autopend-unmatched-return",
			Name:              "Auto-pending configuration receives an unmatched return",
			Description:       "An inbound PPD return references an original trace unknown to this configuration. Twisp posts it to the exception account and marks the file hasExceptions=true.",
			Template:          unmatchedReturnACH,
			AutoPending:       true,
			ExpectedException: true,
		},
		{
			ID:          "ppd-debit-accepted",
			Name:        "PPD debit accepted",
			Description: "CREATE returns SETTLE with an existing end-user account. SETTLE is acknowledged with an empty response.",
			Template:    ppdDebitACH,
			DDA:         "100000001",
			OnCreate:    settleResponse(),
		},
		{
			ID:          "ppd-credit-accepted",
			Name:        "PPD credit accepted",
			Description: "CREATE returns SETTLE with an existing end-user account for a credit entry.",
			Template:    ppdCreditACH,
			DDA:         "100000002",
			OnCreate:    settleResponse(),
		},
		{
			ID:          "iat-debit-accepted",
			Name:        "IAT debit accepted",
			Description: "IAT debit entry using the IAT webhook payload. CREATE returns SETTLE.",
			Template:    iatDebitACH,
			DDA:         "100000003",
			OnCreate:    settleResponse(),
		},
		{
			ID:             "iat-debit-returned",
			Name:           "IAT debit returned",
			Description:    "IAT debit entry returned at CREATE time with customer-supplied addenda99.",
			Template:       iatDebitACH,
			DDA:            "100000004",
			OnCreate:       returnResponse("R01", "IAT return from processor"),
			ExpectedReturn: true,
		},
		{
			ID:            "ppd-debit-retry",
			Name:          "PPD debit retry example",
			Description:   "First CREATE returns RETRY. Twisp retries the webhook, then the processor returns SETTLE.",
			Template:      ppdDebitACH,
			DDA:           "100000005",
			OnCreate:      settleResponse(),
			RetryOnCreate: true,
		},
		{
			ID:             "ppd-debit-pending",
			Name:           "PPD debit pending example",
			Description:    "CREATE returns PENDING. The demo prints the manual workflow calls, then executes RETURN so a return file is generated.",
			Template:       ppdDebitACH,
			DDA:            "100000006",
			OnCreate:       pendingResponse(),
			ManualReturn:   true,
			ReturnCode:     "R20",
			ReturnInfo:     "Manual return after pending review",
			ExpectedReturn: true,
		},
		{
			ID:             "ppd-debit-return-create",
			Name:           "PPD debit returned at create time",
			Description:    "CREATE returns RETURN with customer-supplied return code and addenda99 information.",
			Template:       ppdDebitACH,
			DDA:            "100000007",
			OnCreate:       returnResponse("R01", "No funds from demo"),
			ExpectedReturn: true,
		},
		{
			ID:             "ppd-debit-return-settle",
			Name:           "PPD debit returned at settle time",
			Description:    "CREATE settles to an account. SETTLE returns RETURN with customer-supplied addenda99.",
			Template:       ppdDebitACH,
			DDA:            "100000008",
			OnCreate:       settleResponse(),
			OnSettle:       returnResponse("R01", "Returned during settlement"),
			ExpectedReturn: true,
		},
		{
			ID:             "ppd-debit-unknown-account",
			Name:           "PPD debit unknown account returned",
			Description:    "CREATE returns SETTLE with a random account id. Twisp posts to suspense and generates its default return addenda99.",
			Template:       ppdDebitACH,
			DDA:            "100000009",
			OnCreate:       unknownAccountResponse(),
			ExpectedReturn: true,
		},
		{
			ID:          "ppd-debit-noc",
			Name:        "Adding a NOC addenda98 in response",
			Description: "CREATE settles. SETTLE returns addenda98 so Twisp queues and generates an RDFI NOC file.",
			Template:    ppdDebitACH,
			DDA:         "100000010",
			OnCreate:    settleResponse(),
			OnSettle: hookResponse{Addenda98: map[string]any{
				"changeCode":    "C01",
				"correctedData": "123456789",
			}},
			ExpectedNOC: true,
		},
	}
}

func settleResponse() hookResponse {
	return hookResponse{
		Action:    "SETTLE",
		AccountID: endUserAccountID,
		Metadata: map[string]any{
			"processor": "rdfi-example",
		},
		EntryMetadata: map[string]any{
			"ddaMatched": true,
		},
	}
}

func pendingResponse() hookResponse {
	return hookResponse{Action: "PENDING", AccountID: endUserAccountID}
}

func returnResponse(code, info string) hookResponse {
	return hookResponse{Action: "RETURN", Addenda99: addenda99(code, info)}
}

func unknownAccountResponse() hookResponse {
	return hookResponse{Action: "SETTLE", AccountID: newUUID()}
}

func addenda99(code, info string) map[string]any {
	return map[string]any{"returnCode": code, "addendaInformation": info}
}

func selectScenarios(all []*scenario, pattern string) ([]*scenario, error) {
	if pattern == "" {
		return all, nil
	}
	re, err := regexp.Compile("(?i)" + pattern)
	if err != nil {
		return nil, err
	}
	var selected []*scenario
	for _, sc := range all {
		if re.MatchString(sc.ID) || re.MatchString(sc.Name) {
			selected = append(selected, sc)
		}
	}
	if len(selected) == 0 {
		ids := make([]string, len(all))
		for i, sc := range all {
			ids[i] = sc.ID
		}
		sort.Strings(ids)
		return nil, fmt.Errorf("no scenarios matched %q; available: %s", pattern, strings.Join(ids, ", "))
	}
	return selected, nil
}

func makeACHFile(template, dda string) string {
	if dda == "" {
		return strings.TrimRight(template, "\n") + "\n"
	}
	lines := strings.Split(strings.TrimRight(template, "\n"), "\n")
	iat := false
	for _, line := range lines {
		if strings.Contains(line, "IAT") {
			iat = true
			break
		}
	}
	for i, line := range lines {
		if len(line) < 80 || line[0] != '6' {
			continue
		}
		if iat {
			lines[i] = replaceField(line, 39, 74, dda)
		} else {
			lines[i] = replaceField(line, 12, 29, dda)
		}
	}
	return strings.Join(lines, "\n") + "\n"
}

func replaceField(line string, start, end int, value string) string {
	if len(line) < end {
		return line
	}
	width := end - start
	if len(value) > width {
		value = value[:width]
	}
	return line[:start] + value + strings.Repeat(" ", width-len(value)) + line[end:]
}

func printManualWorkflow(req hookRequest) {
	code := "ACH_RDFI_DR"
	if strings.Contains(req.WorkflowName, "CR") {
		code = "ACH_RDFI_CR"
	}
	fmt.Println("manual touch point:")
	fmt.Printf("Return this pending entry with workflow.execute executionId=%s code=%s task=RETURN params={effective, addenda99}\n", req.ExecutionID, code)
	fmt.Printf("Settle this pending entry with workflow.execute executionId=%s code=%s task=SETTLE params={effective}\n", req.ExecutionID, code)
}

func stringField(m map[string]any, keys ...string) string {
	for _, key := range keys {
		if v, ok := m[key]; ok {
			if s, ok := v.(string); ok {
				return s
			}
		}
	}
	return ""
}

func stringMap(in map[string]any) map[string]string {
	out := make(map[string]string)
	for k, v := range in {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}
	return out
}

func printJSON(v any) {
	var b []byte
	switch val := v.(type) {
	case json.RawMessage:
		b, _ = json.MarshalIndent(json.RawMessage(val), "", "  ")
	default:
		b, _ = json.MarshalIndent(v, "", "  ")
	}
	fmt.Println(string(b))
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func shortID() string {
	id := newUUID()
	return strings.ReplaceAll(id[:13], "-", "")
}

func newUUID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

const ppdDebitACH = `101 03130001202313801041908161055A094101Federal Reserve Bank   My Bank Name           12345678
5225Name on Account                     231380104 PPDREG.SALARY      190816   1121042880000001
627231380104123456789        0200000000               Debit Account           0121042880000001
82250000010023138010000200000000000000000000231380104                          121042880000001
9000001000001000000010023138010000200000000000000000000                                       
9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999
9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999
9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999
9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999
9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999`

const ppdCreditACH = `101 03130001202313801041908161055A094101Federal Reserve Bank   My Bank Name           12345678
5220Name on Account                     231380104 PPDREG.SALARY      190816   1121042880000001
622231380104987654321        0100000000               Credit Account 1        0121042880000002
82200000010023138010000000000000000100000000231380104                          121042880000001
9000001000001000000010023138010000000000000000100000000                                       
9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999
9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999
9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999
9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999
9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999`

var iatDebitACH = strings.Join([]string{
	"101 231380104 1210428822603100000A094101ACME BANK, N.A.        ASF APPLICATION SUPERVI" + strings.Repeat(" ", 8),
	"5225                FF3               US770510487CIATIAT PAYPALUSDUSD2603060651231380100000001",
	"6272313801040007             0000002826695954481211                           1231380105675441",
	"710WEB000000000000002826                      CONSTANCE FELDMAN                        5675441",
	"711ASTROLINE                          14 PANAGIOTI TSANGAR 1ST FLOOR, OFF              5675441",
	"712GERMASOGEIA*LEMESOS\\               CY*4047\\                                         5675441",
	"713WELLS FARGO BANK                   01091000019                         US           5675441",
	"714ACME BANK, N.A.                    01231380104                         US           5675441",
	"7151048710156457  12710 SE SHERMAN ST                                                  5675441",
	"716PORTLAND*OR\\                       US*97233\\                                        5675441",
	"82250000080023138010000000002826000000000000770510487C                         231380100000001",
	"9000001000001000000080023138010000000002826000000000000" + strings.Repeat(" ", 39),
	"9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999",
	"9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999",
}, "\n")

// unmatchedReturnACH contains a regular PPD return whose Addenda 99 original
// trace (231380100064431) was never originated by this fresh configuration.
// The return therefore has no workflow execution to settle and is posted to
// the configured exception account instead.
var unmatchedReturnACH = strings.Join([]string{
	"101 231380104 1210428822605221343D094101ACME BANK, N.A.        ASF APPLICATION SUPERVI" + strings.Repeat(" ", 8),
	"5200PARSN                               9876543210PPDPAYMENT   SD06202605211421231380100033187",
	"6262313801044820617395       0000020000               John Huntley            1231380109557577",
	"799R0123138010006443100000003130001                                            231380109557577",
	"820000000200231380100000000200000000000000009876543210                         231380100033187",
	"9000001000001000000020023138010000000020000000000000000" + strings.Repeat(" ", 39),
	"9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999",
	"9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999",
	"9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999",
	"9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999",
}, "\n")
