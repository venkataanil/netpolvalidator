package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"
)

type connection struct {
	Addresses []string `json:"addresses"`
	Ports     []int    `json:"ports"`
}

var connections map[string][]connection

type connTest struct {
	Address string `json:"address"`
	Port int `json:"port"`
	IngressIdx int `json:"connectionidx"`
	NpName string `json:"npname"`
	Timestamp time.Time `json:"timestamp"`
}

const parallelConnections = 20
var (
	resultsLock sync.Mutex
	connTestLock sync.Mutex
	results     = make([]connTest, 0)
	wg          sync.WaitGroup
	failedConnChan = make(chan connTest, 1000)
	netpolTimeout = time.Second * 10
	httpClient = http.Client{Timeout: 2 * time.Second,}
	gotConnectins = make(chan bool)
)

var allConnTests []connTest

func curlRequest(address string, port int) (bool, time.Time) {
	url := fmt.Sprintf("http://%s:%d", address, port)
	log.Printf("Sending request to address %s", address)
	resp, err := httpClient.Get(url)
	if err != nil {
		log.Printf("Error connecting to address %v %v", address, err)
		return false, time.Now().UTC()
	}
	defer resp.Body.Close()
	log.Printf("Got %v response to address %s", resp.StatusCode, address)
	return resp.StatusCode == http.StatusOK, time.Now().UTC()
}

func testConnections(connTests []connTest) ([]connTest, []connTest){
	var successConn []connTest
	var failedConn []connTest
	for _, ct := range connTests {
		for attempt := 1; attempt <= 3; attempt++ {
			success, timestamp := curlRequest(ct.Address, ct.Port)
			if success {
				ct.Timestamp = timestamp
				successConn = append(successConn, ct)
				break
			} else if attempt == 3 {
				failedConn = append(failedConn, ct)
				log.Printf("failed request to %v after 3 attempts", ct.Address)
			}
		}
	}
	if len(successConn) > 0 {
		resultsLock.Lock()
		for _, conn := range successConn {
			results = append(results, conn)
		}
		resultsLock.Unlock()
	}
	return successConn, failedConn
}

// Wait if the job started creating objects by testing first 5 network policies
func waitForJobStarted(connTests []connTest) {
	var jobStartConn map[string]connTest = make(map[string]connTest)
	addConn := 1
	success := false
	for _, conn := range connTests {
	log.Printf("ANIL conn %v", conn)
		if _, ok := jobStartConn[conn.NpName]; !ok {
			log.Printf("ANIL adding ns %v and conn %v", conn.NpName, conn)
			jobStartConn[conn.NpName] = conn
			if addConn == 5 {
				break
			}
			addConn += 1
		}
		log.Printf("ANIL addConn %d jobStartConn %v", addConn, jobStartConn)
	}
	for {
		for _, conn := range jobStartConn {
			log.Printf("ANIL sending curlRequest to addr %v port %v", conn.Address, conn.Port)
			success, _ = curlRequest(conn.Address, conn.Port)
			if success {
				break
			}
		}
		if success {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func processFailed() {
        batchSize := 10
	semaphore := make(chan struct{}, parallelConnections)
	for {
            batch := make([]connTest, 0, batchSize)
            for i := 0; i < batchSize; i++ {
                select {
                case val := <-failedConnChan:
                    batch = append(batch, val)
                default:
                    // No more data available, process what we have
                    break
                }
            }
            if len(batch) > 0 {
		semaphore <- struct{}{}
		go func(batch []connTest, semaphore chan struct{}) {
			defer func() { <- semaphore }()
                	_, failedConn := testConnections(batch)
			if len(failedConn) > 0 {
				for _, c := range failedConn {
					failedConnChan <- c
				}
			}
		}(batch, semaphore)
            }
            time.Sleep(100 * time.Millisecond) // Avoid tight loop, allows for more data to accumulate
        }
}

func processIngress(connTests []connTest, semaphore chan struct{}, tc int) {
	defer wg.Done()
	defer func() { <- semaphore }()
	done := make(chan bool, 1)
	success := make(chan bool, 1)
	testChan := make(chan []connTest, 1)
	ticker := time.NewTicker(time.Second)
	go func(threadCount int) {
		var failedConn []connTest
		for {
			select {
			case <-done:
				testChan <- failedConn
				return 
			case <-ticker.C:
				_, failedConn = testConnections(connTests)
				if len(failedConn) != len(connTests) {
					log.Println("ticker.C writing failedConn ", threadCount, len(failedConn), len(connTests))
					testChan <- failedConn
					success <- true
					return
				}
			}
		}
	}(tc)
	select {
	case <-success:
	case <-time.After(netpolTimeout):
		log.Println("Timeout reached processing network policy.")
	}
	done <- true
	ticker.Stop()
	// retry failed conn in 3 attempts
	failedConn := <-testChan
	for attempt := 1; attempt <= 3; attempt++ {
		_, failedConn = testConnections(failedConn)
		if len(failedConn) == 0 {
			break
		}
	}
	// if still they fail move them to another thread
	if len(failedConn) > 0 {
		for _, c := range failedConn {
			failedConnChan <- c
		}
	}
}

func resultsHandler(w http.ResponseWriter, r *http.Request) {
	resultsLock.Lock()
	defer resultsLock.Unlock()
	if err := json.NewEncoder(w).Encode(results); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}


func handleRequest(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "Check Request received, processing...")
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Unable to read request body", http.StatusBadRequest)
		return
	}

	err = json.Unmarshal(body, &connections)
	if err != nil {
		http.Error(w, "Unable to parse request body", http.StatusBadRequest)
		return
	}
	log.Println("Start sending Connections info ")
	for npName, rules := range connections {
		for connectionIdx, connection := range rules {
			for _, address := range connection.Addresses {
				for _, port := range connection.Ports {
					allConnTests = append(allConnTests, connTest{Address: address, Port: port, IngressIdx: connectionIdx, NpName: npName})
				}
			}
		}
	}
	r.Body.Close()
	log.Println("ANIL allConnTests %v", allConnTests)
	log.Println("Finished sending Connections info")
	gotConnectins <- true
}

func sendRequests() {
	<-gotConnectins
	log.Println("Start waiting for Network policy object creation ")
	log.Println("ANIL KUMAR allConnTests %v", allConnTests)
	waitForJobStarted(allConnTests)
	log.Println("Finished waiting for Network policy object creation ")
	go processFailed()
	semaphore := make(chan struct{}, parallelConnections)
	for i:= 0; i < len(allConnTests); i += parallelConnections {
		end := i + parallelConnections
		if end > len(allConnTests) {
			end = len(allConnTests)
		}
		semaphore <- struct{}{}
		wg.Add(1)
		log.Println("Created thread: %d for sending curl requests", i)
		go processIngress(allConnTests[i:end], semaphore, i)

	}
	wg.Wait()
}

func main() {
	go sendRequests()
	http.HandleFunc("/check", handleRequest)
	http.HandleFunc("/results", resultsHandler)
	log.Println("Server started on 127.0.0.1:9001")
	go func() {
		log.Fatal(http.ListenAndServe(":9001", nil))
	}()

	select {} // Keep the server running
}

