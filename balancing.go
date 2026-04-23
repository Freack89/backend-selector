// balancing.go
package traefik_backend_selector

import (
    "crypto/sha256"
    "encoding/binary"
    "fmt"
    "hash/fnv"
    "math/rand"
    "net/http"
    "sort"
    "strings"
)

// selectHealthyBackend selects a healthy backend
func (b *BackendSelector) selectHealthyBackend(req *http.Request) string {
    availableBackends := b.getAvailableBackends()
    
    if len(availableBackends) == 0 {
        return ""
    }
    
    if len(availableBackends) == 1 {
        return availableBackends[0].URL
    }
    
    switch b.strategy {
    case "hrw":
        return b.selectHRW(req, availableBackends)
    case "consistent-hash":
        return b.selectConsistentHash(req, availableBackends)
    case "round-robin":
        return b.selectRoundRobin(availableBackends)
    case "random":
        return b.selectRandom(availableBackends)
    default:
        return b.selectHRW(req, availableBackends)
    }
}

// HRW (Rendezvous Hashing)
func (b *BackendSelector) selectHRW(req *http.Request, backends []Backend) string {
    key := b.getHashKey(req)
    
    type scoredBackend struct {
        url   string
        score uint64
    }
    
    var scores []scoredBackend
    for _, backend := range backends {
        score := b.hrwScore(key, backend.URL) * uint64(backend.Weight)
        scores = append(scores, scoredBackend{backend.URL, score})
    }
    
    maxScore := uint64(0)
    selected := backends[0].URL
    for _, sb := range scores {
        if sb.score > maxScore {
            maxScore = sb.score
            selected = sb.url
        }
    }
    
    return selected
}

func (b *BackendSelector) hrwScore(key, node string) uint64 {
    h := sha256.New()
    h.Write([]byte(key + ":" + node))
    hash := h.Sum(nil)
    return binary.BigEndian.Uint64(hash[:8])
}

// Consistent Hashing
func (b *BackendSelector) selectConsistentHash(req *http.Request, backends []Backend) string {
    key := b.getHashKey(req)
    
    virtualNodes := 150
    type vnode struct {
        hash   uint32
        backend string
    }
    
    var ring []vnode
    for _, backend := range backends {
        nodes := virtualNodes * backend.Weight / 100
        if nodes < 1 {
            nodes = 1
        }
        for i := 0; i < nodes; i++ {
            vkey := fmt.Sprintf("%s:%d", backend.URL, i)
            hash := b.fnvHash(key + ":" + vkey)
            ring = append(ring, vnode{hash, backend.URL})
        }
    }
    
    sort.Slice(ring, func(i, j int) bool {
        return ring[i].hash < ring[j].hash
    })
    
    hash := b.fnvHash(key)
    idx := sort.Search(len(ring), func(i int) bool {
        return ring[i].hash >= hash
    })
    
    if idx == len(ring) {
        idx = 0
    }
    
    return ring[idx].backend
}

// Round Robin with weights
func (b *BackendSelector) selectRoundRobin(backends []Backend) string {
    b.mu.Lock()
    defer b.mu.Unlock()
    
    totalWeight := 0
    for _, backend := range backends {
        totalWeight += backend.Weight
    }
    
    count := b.rrCount % uint64(totalWeight)
    b.rrCount++
    
    current := uint64(0)
    for _, backend := range backends {
        current += uint64(backend.Weight)
        if count < current {
            return backend.URL
        }
    }
    
    return backends[0].URL
}

// Random with weights
func (b *BackendSelector) selectRandom(backends []Backend) string {
    totalWeight := 0
    for _, backend := range backends {
        totalWeight += backend.Weight
    }
    
    r := rand.Intn(totalWeight)
    current := 0
    for _, backend := range backends {
        current += backend.Weight
        if r < current {
            return backend.URL
        }
    }
    
    return backends[0].URL
}

func (b *BackendSelector) getHashKey(req *http.Request) string {
    switch b.hashKey {
    case "client_ip":
        return getClientIP(req)
    case "client_ip+uri":
        return getClientIP(req) + ":" + req.URL.Path
    default:
        return getClientIP(req)
    }
}

func getClientIP(req *http.Request) string {
    if ip := req.Header.Get("X-Real-IP"); ip != "" {
        return ip
    }
    
    if xff := req.Header.Get("X-Forwarded-For"); xff != "" {
        ips := strings.Split(xff, ",")
        return strings.TrimSpace(ips[0])
    }
    
    return strings.Split(req.RemoteAddr, ":")[0]
}

func (b *BackendSelector) fnvHash(s string) uint32 {
    h := fnv.New32a()
    h.Write([]byte(s))
    return h.Sum32()
}

