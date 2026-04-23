// main.go
package traefik_backend_selector

import (
    "context"
    "fmt"
    "net/http"
    "sync"
    "time"

    "github.com/sirupsen/logrus"
)

// Config holds the plugin configuration
type Config struct {
    Backends           []Backend          `json:"backends,omitempty"`
    Strategy           string             `json:"strategy,omitempty"`
    HashKey            string             `json:"hashkey,omitempty"`
    Header             string             `json:"header,omitempty"`
    PassiveHealthCheck PassiveHealthCheck `json:"passiveHealthCheck,omitempty"`
}

// Backend represents a backend server
type Backend struct {
    URL    string `json:"url,omitempty"`
    Weight int    `json:"weight,omitempty"`
}

// PassiveHealthCheck configuration
type PassiveHealthCheck struct {
    Enabled         bool          `json:"enabled,omitempty"`
    MaxFailures     int           `json:"maxFailures,omitempty"`
    FailureCodes    []int         `json:"failureCodes,omitempty"`
    Timeout         time.Duration `json:"timeout,omitempty"`
    RetryTimeout    time.Duration `json:"retryTimeout,omitempty"`
    HalfOpenMaxReqs int           `json:"halfOpenMaxReqs,omitempty"`
}

// BackendHealth tracks health state
type BackendHealth struct {
    url          string
    failures     int
    lastFailure  time.Time
    state        string
    mu           sync.RWMutex
    halfOpenReqs int
}

const (
    StateHealthy  = "healthy"
    StateFailed   = "failed"
    StateHalfOpen = "half-open"
)

// BackendSelector is the middleware plugin
type BackendSelector struct {
    next         http.Handler
    name         string
    backends     []Backend
    strategy     string
    hashKey      string
    header       string
    healthConfig PassiveHealthCheck
    healthStates map[string]*BackendHealth
    mu           sync.RWMutex
    rrCount      uint64
    logger       *logrus.Logger
}

// CreateConfig creates the default plugin configuration
func CreateConfig() *Config {
    return &Config{
        Strategy: "hrw",
        HashKey:  "client_ip",
        Header:   "X-Backend-Url",
        PassiveHealthCheck: PassiveHealthCheck{
            Enabled:         true,
            MaxFailures:     5,
            FailureCodes:    []int{500, 502, 503, 504},
            Timeout:         30 * time.Second,
            RetryTimeout:    60 * time.Second,
            HalfOpenMaxReqs: 3,
        },
    }
}

// New creates a new plugin instance
func New(ctx context.Context, next http.Handler, config *Config, name string) (http.Handler, error) {
    if len(config.Backends) == 0 {
        return nil, fmt.Errorf("no backends configured")
    }

    logger := logrus.New()
    logger.SetLevel(logrus.InfoLevel)
    logger.SetFormatter(&logrus.JSONFormatter{})

    bs := &BackendSelector{
        next:         next,
        name:         name,
        backends:     config.Backends,
        strategy:     config.Strategy,
        hashKey:      config.HashKey,
        header:       config.Header,
        healthConfig: config.PassiveHealthCheck,
        healthStates: make(map[string]*BackendHealth),
        logger:       logger,
    }

    // Initialize health states
    for _, backend := range config.Backends {
        bs.healthStates[backend.URL] = &BackendHealth{
            url:   backend.URL,
            state: StateHealthy,
        }
        logger.WithFields(logrus.Fields{
            "plugin":  name,
            "backend": backend.URL,
            "weight":  backend.Weight,
        }).Info("Backend registered")
    }

    // Start background health cleanup
    if config.PassiveHealthCheck.Enabled {
        go bs.cleanupFailures()
    }

    return bs, nil
}

// ServeHTTP implements the middleware logic
func (b *BackendSelector) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
    // Select healthy backend
    backend := b.selectHealthyBackend(req)
    
    if backend == "" {
        b.logger.WithFields(logrus.Fields{
            "plugin":  b.name,
            "service": req.Host,
            "client":  getClientIP(req),
        }).Error("No healthy backends available")
        
        http.Error(rw, "No healthy backends available", http.StatusServiceUnavailable)
        return
    }
    
    // Add backend header
    req.Header.Set(b.header, backend)
    
    b.logger.WithFields(logrus.Fields{
        "plugin":   b.name,
        "service":  req.Host,
        "backend":  backend,
        "strategy": b.strategy,
        "client":   getClientIP(req),
    }).Debug("Backend selected")
    
    // Save to context
    ctx := context.WithValue(req.Context(), "selected_backend", backend)
    ctx = context.WithValue(ctx, "service_name", req.Host)
    req = req.WithContext(ctx)
    
    // Wrap response writer
    observer := &responseObserver{ResponseWriter: rw}
    
    // Forward request
    b.next.ServeHTTP(observer, req)
    
    // Update health based on response
    if b.healthConfig.Enabled {
        b.recordBackendHealth(backend, observer.status, req.Host)
    }
}

// cleanupFailures periodically resets failure counters
func (b *BackendSelector) cleanupFailures() {
    ticker := time.NewTicker(b.healthConfig.Timeout)
    defer ticker.Stop()
    
    for range ticker.C {
        now := time.Now()
        
        b.mu.RLock()
        for url, health := range b.healthStates {
            health.mu.Lock()
            
            switch health.state {
            case StateFailed:
                if now.Sub(health.lastFailure) > b.healthConfig.RetryTimeout {
                    health.state = StateHalfOpen
                    health.failures = 0
                    health.halfOpenReqs = 0
                    
                    b.logger.WithFields(logrus.Fields{
                        "plugin":  b.name,
                        "backend": url,
                    }).Info("Backend moving to half-open state")
                }
                
            case StateHealthy:
                if health.failures > 0 && now.Sub(health.lastFailure) > b.healthConfig.Timeout {
                    health.failures = 0
                }
            }
            
            health.mu.Unlock()
        }
        b.mu.RUnlock()
    }
}

// responseObserver captures response status
type responseObserver struct {
    http.ResponseWriter
    status      int
    wroteHeader bool
}

func (ro *responseObserver) WriteHeader(code int) {
    if !ro.wroteHeader {
        ro.status = code
        ro.wroteHeader = true
        ro.ResponseWriter.WriteHeader(code)
    }
}

func (ro *responseObserver) Write(b []byte) (int, error) {
    if !ro.wroteHeader {
        ro.WriteHeader(http.StatusOK)
    }
    return ro.ResponseWriter.Write(b)
}

