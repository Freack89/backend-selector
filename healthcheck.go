// healthcheck.go
package traefik_backend_selector

import (
    "time"
)

// recordBackendHealth updates backend health based on response
func (b *BackendSelector) recordBackendHealth(url string, statusCode int, serviceName string) {
    b.mu.RLock()
    health, exists := b.healthStates[url]
    b.mu.RUnlock()
    
    if !exists {
        return
    }
    
    health.mu.Lock()
    defer health.mu.Unlock()
    
    // Check if status code is a failure
    isFailure := false
    for _, code := range b.healthConfig.FailureCodes {
        if statusCode == code {
            isFailure = true
            break
        }
    }
    
    now := time.Now()
    
    if isFailure {
        health.failures++
        health.lastFailure = now
        
        b.logger.WithFields(map[string]interface{}{
            "plugin":      b.name,
            "backend":     url,
            "service":     serviceName,
            "status_code": statusCode,
            "failures":    health.failures,
        }).Warn("Backend failure detected")
        
        if health.state == StateHalfOpen {
            // Immediately mark as failed in half-open state
            health.state = StateFailed
            b.logger.WithFields(map[string]interface{}{
                "plugin":  b.name,
                "backend": url,
                "service": serviceName,
            }).Error("Backend failed in half-open state")
        } else if health.state == StateHealthy && health.failures >= b.healthConfig.MaxFailures {
            // Mark as failed after max failures
            health.state = StateFailed
            b.logger.WithFields(map[string]interface{}{
                "plugin":   b.name,
                "backend":  url,
                "service":  serviceName,
                "failures": health.failures,
            }).Error("Backend marked as failed - circuit breaker opened")
        }
    } else {
        // Successful request
        if health.state == StateHalfOpen {
            health.halfOpenReqs++
            if health.halfOpenReqs >= b.healthConfig.HalfOpenMaxReqs {
                // Enough successful requests - return to healthy
                health.state = StateHealthy
                health.failures = 0
                health.halfOpenReqs = 0
                
                b.logger.WithFields(map[string]interface{}{
                    "plugin":  b.name,
                    "backend": url,
                    "service": serviceName,
                }).Info("Backend recovered - circuit breaker closed")
            }
        } else if health.state == StateHealthy && health.failures > 0 {
            // Reset failures after success
            health.failures = 0
            b.logger.WithFields(map[string]interface{}{
                "plugin":  b.name,
                "backend": url,
                "service": serviceName,
            }).Debug("Backend failures reset after successful request")
        }
    }
}

// getAvailableBackends returns available backends
func (b *BackendSelector) getAvailableBackends() []Backend {
    var available []Backend
    
    b.mu.RLock()
    defer b.mu.RUnlock()
    
    for _, backend := range b.backends {
        health, exists := b.healthStates[backend.URL]
        if !exists {
            available = append(available, backend)
            continue
        }
        
        health.mu.RLock()
        state := health.state
        
        if state == StateHealthy {
            available = append(available, backend)
        } else if state == StateHalfOpen {
            // Allow limited requests in half-open state
            if health.halfOpenReqs < b.healthConfig.HalfOpenMaxReqs {
                health.halfOpenReqs++
                available = append(available, backend)
            }
        }
        health.mu.RUnlock()
    }
    
    return available
}

