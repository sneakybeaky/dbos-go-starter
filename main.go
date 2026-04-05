package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
)

const STEPS_EVENT = "steps_event"

var dbosCtx dbos.DBOSContext

/*****************************/
/**** WORKFLOWS AND STEPS ****/
/*****************************/

func ExampleWorkflow(ctx dbos.DBOSContext, _ string) (string, error) {
	_, err := dbos.RunAsStep(ctx, func(stepCtx context.Context) (string, error) {
		return stepOne(stepCtx)
	})
	if err != nil {
		return "", err
	}
	err = dbos.SetEvent(ctx, STEPS_EVENT, 1)
	if err != nil {
		return "", err
	}
	_, err = dbos.RunAsStep(ctx, func(stepCtx context.Context) (string, error) {
		return stepTwo(stepCtx)
	})
	if err != nil {
		return "", err
	}
	err = dbos.SetEvent(ctx, STEPS_EVENT, 2)
	if err != nil {
		return "", err
	}
	_, err = dbos.RunAsStep(ctx, func(stepCtx context.Context) (string, error) {
		return stepThree(stepCtx)
	})
	if err != nil {
		return "", err
	}
	err = dbos.SetEvent(ctx, STEPS_EVENT, 3)
	if err != nil {
		return "", err
	}
	return "Workflow completed", nil
}

func stepOne(ctx context.Context) (string, error) {
	time.Sleep(5 * time.Second)
	fmt.Println("Step one completed!")
	return "Step 1 completed", nil
}

func stepTwo(ctx context.Context) (string, error) {
	time.Sleep(5 * time.Second)
	fmt.Println("Step two completed!")
	return "Step 2 completed", nil
}

func stepThree(ctx context.Context) (string, error) {
	time.Sleep(5 * time.Second)
	fmt.Println("Step three completed!")
	return "Step 3 completed", nil
}

/*****************************/
/**** Helper Functions *******/
/*****************************/

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

/*****************************/
/**** Main Function **********/
/*****************************/

func main() {
	port := flag.String("port", "8080", "HTTP listen port")
	executorID := flag.String("executor-id", "local", "DBOS executor ID (optional, overridden by DBOS__VMID env var)")
	flag.Parse()

	// Create DBOS context
	var err error
	dbosCtx, err = dbos.NewDBOSContext(context.Background(), dbos.Config{
		DatabaseURL: os.Getenv("DBOS_SYSTEM_DATABASE_URL"),
		AppName:     "dbos-go-starter",
		AdminServer: false,
		ExecutorID:  *executorID,
	})
	if err != nil {
		panic(err)
	}

	// Register workflows
	dbos.RegisterWorkflow(dbosCtx, ExampleWorkflow)

	// Launch DBOS
	err = dbosCtx.Launch()
	if err != nil {
		panic(err)
	}
	defer dbosCtx.Shutdown(10 * time.Second)

	// Initialize standard HTTP mux
	mux := http.NewServeMux()

	// HTTP Handlers
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "./html/app.html")
	})
	mux.HandleFunc("GET /workflow/{taskid}", workflowHandler)
	mux.HandleFunc("GET /last_step/{taskid}", lastStepHandler)
	mux.HandleFunc("POST /crash", crashHandler)

	addr := ":" + *port
	fmt.Printf("Server starting on http://localhost:%s\n", *port)
	err = http.ListenAndServe(addr, mux)
	if err != nil {
		fmt.Printf("Error starting server: %s\n", err)
	}
}

/*****************************/
/**** HTTP HANDLERS **********/
/*****************************/

func workflowHandler(w http.ResponseWriter, r *http.Request) {
	taskID := r.PathValue("taskid")

	if taskID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Task ID is required"})
		return
	}

	_, err := dbos.RunWorkflow(dbosCtx, ExampleWorkflow, "", dbos.WithWorkflowID(taskID))
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
}

func lastStepHandler(w http.ResponseWriter, r *http.Request) {
	taskID := r.PathValue("taskid")

	if taskID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Task ID is required"})
		return
	}

	step, err := dbos.GetEvent[int](dbosCtx, taskID, STEPS_EVENT, 0)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	fmt.Fprintf(w, "%d", step)
}

// This endpoint crashes the application. For demonstration purposes only :)
func crashHandler(w http.ResponseWriter, r *http.Request) {
	os.Exit(1)
}
