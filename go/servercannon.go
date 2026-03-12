package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "mygomodule/cannon" // Import wygenerowanego pakietu protobuf

	"google.golang.org/grpc"
)

// Struktura serwera gRPC
type CannonServer struct {
	pb.UnimplementedCannonServiceServer
	isLog            bool
	isSave           bool
	hostPattern      string
	nodeName         string
	localABlock      [][]float64
	localBBlock      [][]float64
	localCBlock      [][]float64
	neighbors        map[string]string
	step             int32
	totalSteps       int32
	mutex            sync.RWMutex
	neighborData     map[string]map[string][][]float64
	neighborClient   map[string]pb.CannonServiceClient
	nodeRowIndex     int
	nodeColIndex     int
	totalNodesRows   int
	totalNodesCols   int
	allNodes         []string
	allClients       map[string]pb.CannonServiceClient
	readyNodes       map[string]bool
	doneNodes        map[string]bool
	neighborStepAcks map[string]int32
	shiftedBlocks    map[string][]float64
	receivedBlocks   map[string]map[string]map[string][][]float64
	receivedBlocksCh map[string]map[string]chan [][]float64
	nodeIPs          []string
}

// Metoda pomocnicza do wyświetlania logów
func (s *CannonServer) debugLogf(format string, v ...interface{}) {
	if s.isLog {
		log.Printf(format, v...)
	}
}

func oppositeDirection(direction string) string {
	switch direction {
	case "up":
		return "down"
	case "down":
		return "up"
	case "left":
		return "right"
	case "right":
		return "left"
	default:
		return ""
	}
}

func serializeMatrix(matrix [][]float64) []float64 {
	size := len(matrix)
	data := make([]float64, size*size)
	for i := 0; i < size; i++ {
		for j := 0; j < size; j++ {
			data[i*size+j] = matrix[i][j]
		}
	}
	return data
}

func deserializeMatrix(data []float64) [][]float64 {
	size := int(math.Sqrt(float64(len(data))))
	if size*size != len(data) {
		log.Fatalf("Nieprawidłowe dane macierzy: oczekiwano %d elementów, otrzymano %d", size*size, len(data))
	}

	matrix := make([][]float64, size)
	for i := 0; i < size; i++ {
		matrix[i] = make([]float64, size)
		for j := 0; j < size; j++ {
			matrix[i][j] = data[i*size+j]
		}
	}
	return matrix
}

func (s *CannonServer) SendBlock(ctx context.Context, req *pb.SendBlockRequest) (*pb.SendBlockResponse, error) {
	receivedMatrix := deserializeMatrix(req.Data)
	blockType := req.BlockType
	dir := oppositeDirection(req.Direction) // Odbiorca otrzymuje z przeciwnym kierunku

	s.debugLogf("Węzeł %s: SendBlock: Otrzymano blok %s od %s w kierunku %s", s.nodeName, blockType, req.NodeName, dir)

	// Umieszczamy otrzymany blok w kanale
	s.receivedBlocksCh[blockType][dir] <- receivedMatrix

	return &pb.SendBlockResponse{}, nil
}

func (s *CannonServer) shiftBlockSend(direction string, blockType string, block [][]float64) {
	neighborName := s.neighbors[direction]
	neighborClient, exists := s.neighborClient[neighborName]
	if !exists || neighborClient == nil {
		s.debugLogf("Węzeł %s: shiftBlockSend: Brak klienta gRPC dla sąsiada %s", s.nodeName, neighborName)
		return
	}

	data := serializeMatrix(block)

	s.debugLogf("Węzeł %s: shiftBlockSend: Wysyłamy blok %s w kierunku %s do %s: %v", s.nodeName, blockType, direction, neighborName, block)
	req := &pb.SendBlockRequest{
		NodeName:  s.nodeName,
		Data:      data,
		Direction: direction,
		BlockType: blockType,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := neighborClient.SendBlock(ctx, req)
	if err != nil {
		s.debugLogf("Węzeł %s: shiftBlockSend: Błąd podczas wysyłania bloku %s do %s: %v", s.nodeName, blockType, neighborName, err)
	}
}

func (s *CannonServer) shiftBlockReceive(direction string, blockType string) [][]float64 {
	s.debugLogf("Węzeł %s: shiftBlockReceive: Oczekiwanie na blok %s z kierunku %s", s.nodeName, blockType, direction)
	select {
	case block := <-s.receivedBlocksCh[blockType][direction]:
		s.debugLogf("Węzeł %s: shiftBlockReceive: Otrzymano blok %s z kierunku %s: %v", s.nodeName, blockType, direction, block)
		return block
	case <-time.After(30 * time.Second):
		s.debugLogf("Węzeł %s: shiftBlockReceive: Timeout podczas oczekiwania na blok %s z kierunku %s", s.nodeName, blockType, direction)
		return nil
	}
}

func (s *CannonServer) exchangeBlocks() {
	s.debugLogf("Węzeł %s: exchangeBlocks: Wysyłanie A w lewo i B w górę", s.nodeName)
	s.shiftBlockSend("left", "A", s.localABlock)
	s.shiftBlockSend("up", "B", s.localBBlock)

	s.debugLogf("Węzeł %s: exchangeBlocks: Odbiór A z prawej i B z dołu", s.nodeName)
	newA := s.shiftBlockReceive("right", "A")
	newB := s.shiftBlockReceive("down", "B")
	if newA == nil {
		s.debugLogf("Węzeł %s: exchangeBlocks: Otrzymano nil jako nowy blok A z prawej!", s.nodeName)
	}
	if newB == nil {
		s.debugLogf("Węzeł %s: exchangeBlocks: Otrzymano nil jako nowy blok B z dołu!", s.nodeName)
	}
	s.localABlock = newA
	s.localBBlock = newB

	s.syncStepWithNeighbors()
}

func (s *CannonServer) SyncStep(ctx context.Context, req *pb.SyncRequest) (*pb.SyncResponse, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Sprawdzamy, czy nadawca jest sąsiadem
	if _, isNeighbor := s.neighborClient[req.NodeName]; isNeighbor {
		s.neighborStepAcks[req.NodeName] = req.Step
	}

	return &pb.SyncResponse{
		Step: s.step,
	}, nil
}

func (s *CannonServer) syncStepWithNeighbors() {
	s.debugLogf("Węzeł %s: Synchronizacja kroku %d z sąsiadami.", s.nodeName, s.step)
	var wg sync.WaitGroup

	s.mutex.Lock()
	currentStep := s.step
	s.mutex.Unlock()

	// Wysyłamy SyncStep tylko do sąsiadów
	for neighborName, client := range s.neighborClient {
		if client == nil {
			continue
		}
		wg.Add(1)
		go func(neighborName string, client pb.CannonServiceClient) {
			defer wg.Done()
			req := &pb.SyncRequest{
				NodeName: s.nodeName,
				Step:     currentStep,
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := client.SyncStep(ctx, req)
			if err != nil {
				s.debugLogf("Węzeł %s: Błąd podczas sync z %s: %v", s.nodeName, neighborName, err)
			}
		}(neighborName, client)
	}
	wg.Wait()

	// Czekamy aż sąsiedzi osiągną ten sam krok
	for {
		s.mutex.Lock()
		allSynced := true
		for neighborName := range s.neighborClient {
			if s.neighborStepAcks[neighborName] < currentStep {
				allSynced = false
				break
			}
		}
		s.mutex.Unlock()

		if allSynced {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	s.debugLogf("Węzeł %s: Synchronizacja kroku %d z sąsiadami zakończona.", s.nodeName, currentStep)
}

func (s *CannonServer) NodeReady(ctx context.Context, req *pb.ReadyRequest) (*pb.ReadyResponse, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.debugLogf("Węzeł %s: Otrzymano NodeReady od %s.", s.nodeName, req.NodeName)

	s.readyNodes[req.NodeName] = true

	readyNodesList := []string{}
	for node, ready := range s.readyNodes {
		if ready {
			readyNodesList = append(readyNodesList, node)
		}
	}

	return &pb.ReadyResponse{
		ReadyNodes: readyNodesList,
	}, nil
}

func (s *CannonServer) waitForAllNodesReady() {
	var wg sync.WaitGroup

	s.mutex.Lock()
	s.readyNodes = make(map[string]bool)
	s.readyNodes[s.nodeName] = true
	allNodesCount := len(s.allNodes)
	s.mutex.Unlock()

	for nodeName, client := range s.allClients {
		if nodeName == s.nodeName || client == nil {
			continue
		}
		wg.Add(1)
		go func(nodeName string, client pb.CannonServiceClient) {
			defer wg.Done()
			req := &pb.ReadyRequest{
				NodeName: s.nodeName,
			}
			for {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				resp, err := client.NodeReady(ctx, req)
				cancel()
				if err != nil {
					s.debugLogf("Węzeł %s: Błąd podczas komunikacji z %s w waitForAllNodesReady: %v", s.nodeName, nodeName, err)
					time.Sleep(1 * time.Second)
					continue
				}

				s.mutex.Lock()
				for _, readyNode := range resp.ReadyNodes {
					s.readyNodes[readyNode] = true
				}
				s.mutex.Unlock()
				break
			}
		}(nodeName, client)
	}

	wg.Wait()

	// Teraz czekamy aż wszystkie węzły będą gotowe
	for {
		s.mutex.Lock()
		readyCount := len(s.readyNodes)
		s.mutex.Unlock()

		if readyCount == allNodesCount {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	s.debugLogf("Węzeł %s: Wszystkie węzły są gotowe. Rozpoczynamy obliczenia.", s.nodeName)
}

func (s *CannonServer) NodeDone(ctx context.Context, req *pb.DoneRequest) (*pb.DoneResponse, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.debugLogf("Węzeł %s: Otrzymano NodeDone od %s.", s.nodeName, req.NodeName)
	s.doneNodes[req.NodeName] = true

	var doneList []string
	for node, done := range s.doneNodes {
		if done {
			doneList = append(doneList, node)
		}
	}

	return &pb.DoneResponse{DoneNodes: doneList}, nil
}

func (s *CannonServer) waitForAllNodesDone() {
	var wg sync.WaitGroup

	s.mutex.Lock()
	s.doneNodes[s.nodeName] = true
	total := len(s.allNodes)
	s.mutex.Unlock()

	for nodeName, client := range s.allClients {
		if nodeName == s.nodeName || client == nil {
			continue
		}
		wg.Add(1)
		go func(nodeName string, client pb.CannonServiceClient) {
			defer wg.Done()
			req := &pb.DoneRequest{NodeName: s.nodeName}
			for {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				resp, err := client.NodeDone(ctx, req)
				cancel()
				if err != nil {
					s.debugLogf("Węzeł %s: błąd NodeDone do %s: %v", s.nodeName, nodeName, err)
					time.Sleep(1 * time.Second)
					continue
				}
				s.mutex.Lock()
				for _, dn := range resp.DoneNodes {
					s.doneNodes[dn] = true
				}
				s.mutex.Unlock()
				break
			}
		}(nodeName, client)
	}

	wg.Wait()
	for {
		s.mutex.Lock()
		if len(s.doneNodes) == total {
			s.mutex.Unlock()
			break
		}
		s.mutex.Unlock()
		time.Sleep(500 * time.Millisecond)
	}

	s.debugLogf("Węzeł %s: Wszystkie węzły zakończyły obliczenia.", s.nodeName)
}

func (s *CannonServer) initializeConnections(port string) {
	neighborSet := make(map[string]bool)
	for _, neighborName := range s.neighbors {
		neighborSet[neighborName] = true
	}

	for _, nodeName := range s.allNodes {
		if nodeName == s.nodeName {
			continue // Pomijamy połączenie do samego siebie
		}

		neighborIndex, err := strconv.Atoi(strings.Split(nodeName, " ")[1])
		if err != nil {
			log.Fatalf("Nieprawidłowa nazwa węzła: %v", err)
		}

		var address string
		if len(s.nodeIPs) > 0 {
			if neighborIndex-1 < len(s.nodeIPs) {
				address = fmt.Sprintf("%s:%s", strings.TrimSpace(s.nodeIPs[neighborIndex-1]), port)
			} else {
				log.Fatalf("Brak adresu IP dla węzła %s (indeks %d poza zakresem)", nodeName, neighborIndex)
			}
		} else {
			address = fmt.Sprintf("%s:%s", fmt.Sprintf(s.hostPattern, neighborIndex), port)
		}

		s.debugLogf("Węzeł %s - łączę się z %s pod adresem %s", s.nodeName, nodeName, address)

		var conn *grpc.ClientConn
		var errDial error
		maxRetries := 10
		success := false
		for i := 0; i < maxRetries; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			conn, errDial = grpc.DialContext(ctx, address,
				grpc.WithInsecure(),
				grpc.WithBlock(),
				grpc.WithDefaultCallOptions(
					grpc.MaxCallRecvMsgSize(2*1024*1024*1024), // 2 GB
					grpc.MaxCallSendMsgSize(2*1024*1024*1024), // 2 GB
				),
			)
			cancel()
			if errDial != nil {
				s.debugLogf("Węzeł %s: Nie można połączyć z %s (%s): %v", s.nodeName, nodeName, address, errDial)
				time.Sleep(1 * time.Second)
				continue
			}
			client := pb.NewCannonServiceClient(conn)
			s.allClients[nodeName] = client

			// Sprawdź, czy jest sąsiadem
			if neighborSet[nodeName] {
				s.neighborClient[nodeName] = client
				s.debugLogf("Węzeł %s: Nawiązano połączenie z sąsiadem %s.", s.nodeName, nodeName)
			} else {
				s.debugLogf("Węzeł %s: Nawiązano połączenie z węzłem %s (nie sąsiad).", s.nodeName, nodeName)
			}

			success = true
			break
		}
		if !success {
			log.Fatalf("Węzeł %s: Nie udało się nawiązać połączenia z węzłem %s po %d próbach.", s.nodeName, nodeName, maxRetries)
		}
	}

	// Po zainicjalizowaniu sąsiadów, wypisz ich
	for dir, neighborName := range s.neighbors {
		s.debugLogf("Węzeł %s: Sąsiad w kierunku %s to %s", s.nodeName, dir, neighborName)
	}
}

func intSqrt(n int) int {
	sqrtN := int(math.Sqrt(float64(n)))
	if sqrtN*sqrtN != n {
		log.Fatalf("Całkowita liczba węzłów (%d) nie jest kwadratem liczby całkowitej.", n)
	}
	return sqrtN
}

func (s *CannonServer) initializeZeroMatrix() [][]float64 {
	size := s.getBlockSize()
	matrix := make([][]float64, size)
	for i := 0; i < size; i++ {
		matrix[i] = make([]float64, size)
	}
	return matrix
}

func (s *CannonServer) getBlockSize() int {
	if len(s.localABlock) > 0 {
		return len(s.localABlock)
	}
	log.Fatal("Nie można ustalić rozmiaru bloku, ponieważ macierz A nie jest zainicjalizowana")
	return 0
}

func main() {
	isLog := flag.Bool("isLog", false, "Czy wyświetlać logi")
	hostPattern := flag.String("hostPattern", "go-node-%d", "Wzorzec adresów dla węzłów (%d to indeks węzła)")
	isSave := flag.Bool("isSave", true, "Czy zapisać wynik do pliku")

	nodeIPsStr := flag.String("nodeIPs", "", "Lista adresów (lub hostów) węzłów oddzielonych przecinkami. Jeśli podana, zastępuje hostPattern")

	flag.Parse()

	nodeName := os.Getenv("NODE_NAME")
	port := os.Getenv("SERVER_PORT")
	totalNodesStr := os.Getenv("TOTAL_NODES")

	if nodeName == "" || port == "" {
		log.Fatal("Zmienne środowiskowe NODE_NAME i SERVER_PORT muszą być ustawione")
	}

	totalNodes := 9
	if totalNodesStr != "" {
		var err error
		totalNodes, err = strconv.Atoi(totalNodesStr)
		if err != nil {
			log.Fatalf("Nieprawidłowa wartość TOTAL_NODES: %v", err)
		}
	}

	nodeIndex, err := strconv.Atoi(strings.Split(nodeName, " ")[1])
	if err != nil {
		log.Fatalf("Nieprawidłowy numer w NODE_NAME: %v", err)
	}

	// Sprawdzamy, czy totalNodes to kwadrat
	totalNodesRows := intSqrt(totalNodes)
	totalNodesCols := totalNodesRows

	// Tworzymy listę wszystkich węzłów
	totalNodesList := make([]string, totalNodes)
	for i := 1; i <= totalNodes; i++ {
		totalNodesList[i-1] = fmt.Sprintf("Node %d", i)
	}

	// Obliczamy rowIndex, colIndex w siatce
	nodeRowIndex := (nodeIndex - 1) / totalNodesCols
	nodeColIndex := (nodeIndex - 1) % totalNodesCols

	// Parsowanie nodeIPs - jeśli flaga została podana
	var nodeIPs []string
	if *nodeIPsStr != "" {
		parts := strings.Split(*nodeIPsStr, ",")
		for _, part := range parts {
			nodeIPs = append(nodeIPs, strings.TrimSpace(part))
		}
	}

	server := &CannonServer{
		isLog:            *isLog,
		isSave:           *isSave,
		hostPattern:      *hostPattern,
		nodeName:         nodeName,
		step:             0,
		totalSteps:       int32(totalNodesRows),
		neighbors:        getNeighbors(nodeRowIndex, nodeColIndex, totalNodesRows, totalNodesCols),
		neighborData:     make(map[string]map[string][][]float64),
		neighborClient:   make(map[string]pb.CannonServiceClient),
		nodeRowIndex:     nodeRowIndex,
		nodeColIndex:     nodeColIndex,
		totalNodesRows:   totalNodesRows,
		totalNodesCols:   totalNodesCols,
		allClients:       make(map[string]pb.CannonServiceClient),
		readyNodes:       make(map[string]bool),
		doneNodes:        make(map[string]bool),
		neighborStepAcks: make(map[string]int32),
		allNodes:         totalNodesList,
		shiftedBlocks:    make(map[string][]float64),
		receivedBlocks:   make(map[string]map[string]map[string][][]float64),
		nodeIPs:          nodeIPs,
	}

	// Kanały do odbierania bloków
	server.receivedBlocksCh = make(map[string]map[string]chan [][]float64)
	blockTypes := []string{"A", "B"}
	directions := []string{"up", "down", "left", "right"}
	for _, blockType := range blockTypes {
		server.receivedBlocksCh[blockType] = make(map[string]chan [][]float64)
		for _, dir := range directions {
			server.receivedBlocksCh[blockType][dir] = make(chan [][]float64, 1) // Bufor 1
		}
	}

	for _, name := range server.allNodes {
		if name != server.nodeName {
			server.neighborStepAcks[name] = -1 // Inicjalizujemy na -1
		}
	}

	log.Printf("Węzeł %s: Uruchomienie programu.", nodeName)

	dataStart := time.Now()
	// Wczytujemy pliki A i B (np. A<nodeIndex>.txt, B<nodeIndex>.txt)
	server.localABlock, server.localBBlock = server.initializeLocalBlocks(nodeIndex)
	// Zakończenie pomiaru
	dataElapsed := time.Since(dataStart)
	log.Printf("Węzeł %s: Czas wczytywania bloków A/B = %v s", nodeName, dataElapsed.Seconds())

	// Inicjalizujemy macierz C zerami
	server.localCBlock = server.initializeZeroMatrix()

	// Uruchomienie serwera gRPC
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Nie można nasłuchiwać na porcie %s: %v", port, err)
	}
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(2*1024*1024*1024), // 2 GB
		grpc.MaxSendMsgSize(2*1024*1024*1024), // 2 GB
	)
	pb.RegisterCannonServiceServer(grpcServer, server)
	log.Printf("Węzeł %s: Uruchamianie serwera gRPC na porcie %s", server.nodeName, port)

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("grpcServer.Serve zakończony: %v", err)
		}
	}()

	time.Sleep(2 * time.Second)

	// Inicjalizacja połączeń z sąsiadami
	server.initializeConnections(port)

	// Synchronizacja gotowości
	server.waitForAllNodesReady()

	time.Sleep(5 * time.Second)

	// Uruchomienie algorytmu Cannona
	compStart := time.Now()
	server.runCannonAlgorithm()
	compElapsed := time.Since(compStart)
	log.Printf("Węzeł %s: Czas wykonywania algorytmu Cannona = %v s", nodeName, compElapsed.Seconds())

	server.waitForAllNodesDone()

	// Sprzątanie i zamknięcie
	grpcServer.Stop()
	log.Printf("Węzeł %s: Proces zakończony.", server.nodeName)
	os.Exit(0)
}

func (s *CannonServer) runCannonAlgorithm() {
	s.debugLogf("Węzeł %s: Rozpoczynamy algorytm Cannona. TotalSteps=%d", s.nodeName, s.totalSteps)

	// Wykonujemy initialShift na starcie
	s.initialShift()

	for step := int32(0); step < s.totalSteps; step++ {
		s.mutex.Lock()
		s.step = step
		s.mutex.Unlock()

		// Dla kroków > 0 przesuwamy bloki przed mnożeniem
		if step > 0 {
			s.debugLogf("Węzeł %s: Krok %d: Przesuwamy bloki A i B.", s.nodeName, step)
			s.exchangeBlocks()
		}

		s.debugLogf("Węzeł %s: *** Krok %d: Rozpoczynamy mnożenie bloków ***", s.nodeName, step)
		partialResult := s.multiplyLocalBlocks()

		s.debugLogf("Węzeł %s: Krok %d: Wynik mnożenia przed dodaniem do C: %v", s.nodeName, step, partialResult)
		s.addToLocalCBlock(partialResult)

		s.debugLogf("Węzeł %s: Krok %d zakończony, C: %v", s.nodeName, step, s.localCBlock)
	}

	if s.isSave {
		s.saveLocalCBlock()
	}
	log.Printf("Węzeł %s: Algorytm Cannona zakończony.", s.nodeName)
}

func (s *CannonServer) initialShift() {
	s.debugLogf("Węzeł %s: Rozpoczynamy initialShift. Początkowe A=%v, B=%v", s.nodeName, s.localABlock, s.localBBlock)
	// Przesunięcia wstępne dla A
	for i := 0; i < s.nodeRowIndex; i++ {
		s.debugLogf("Węzeł %s: initialShift: Przesunięcie A w lewo nr %d", s.nodeName, i+1)
		s.shiftBlockSend("left", "A", s.localABlock)
		newA := s.shiftBlockReceive("right", "A")
		if newA == nil {
			s.debugLogf("Węzeł %s: initialShift: Otrzymano nil jako nowy blok A po przesunięciu w lewo, iteracja %d", s.nodeName, i+1)
		}
		s.localABlock = newA
	}

	// Przesunięcia wstępne dla B
	for j := 0; j < s.nodeColIndex; j++ {
		s.debugLogf("Węzeł %s: initialShift: Przesunięcie B w górę nr %d", s.nodeName, j+1)
		s.shiftBlockSend("up", "B", s.localBBlock)
		newB := s.shiftBlockReceive("down", "B")
		if newB == nil {
			s.debugLogf("Węzeł %s: initialShift: Otrzymano nil jako nowy blok B po przesunięciu w górę, iteracja %d", s.nodeName, j+1)
		}
		s.localBBlock = newB
	}

	s.syncStepWithNeighbors()
	s.debugLogf("Węzeł %s: initialShift zakończony. A=%v, B=%v", s.nodeName, s.localABlock, s.localBBlock)
}

func (s *CannonServer) multiplyLocalBlocks() [][]float64 {
	s.mutex.RLock()
	a := s.localABlock
	b := s.localBBlock
	s.mutex.RUnlock()

	// Wymiary lokalnego bloku (zakładamy, że a i b to kwadratowe macierze NxN)
	size := len(a)

	// Alokujemy wynik NxN
	result := make([][]float64, size)
	for i := 0; i < size; i++ {
		result[i] = make([]float64, size)
	}

	// Ustalamy liczbę "pracowników" (goroutine), np. na liczbę CPU
	workers := runtime.NumCPU()

	// Każdy pracownik dostaje pewien przedział wierszy do obliczenia:
	rowsPerWorker := size / workers
	if rowsPerWorker == 0 {
		// Dla bezpieczeństwa, jeżeli size < workers, to wstawiamy 1 wiersz na worker
		rowsPerWorker = 1
	}

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		from := w * rowsPerWorker
		to := from + rowsPerWorker
		if w == workers-1 {
			// Ostatni worker bierze wszystkie pozostałe wiersze
			to = size
		}
		if from >= size {
			break
		}

		wg.Add(1)
		go func(from, to int) {
			defer wg.Done()
			for i := from; i < to; i++ {
				// Wyciągamy wiersz macierzy a i wynikowy wiersz result[i] na zewnątrz pętli k/j
				ai := a[i]
				ci := result[i]

				// Mnożenie w kolejności i -> k -> j
				for k := 0; k < size; k++ {
					aik := ai[k] // raz odczytujemy a[i][k]
					bk := b[k]   // raz pobieramy wskaźnik do wiersza b[k]
					for j := 0; j < size; j++ {
						ci[j] += aik * bk[j]
					}
				}
			}
		}(from, to)
	}

	// Czekamy na wszystkie goroutine
	wg.Wait()

	return result
}

func (s *CannonServer) addToLocalCBlock(partialResult [][]float64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.debugLogf("Węzeł %s: addToLocalCBlock: Dodajemy do C: %v, C przed dodaniem: %v", s.nodeName, partialResult, s.localCBlock)
	size := len(partialResult)
	for i := 0; i < size; i++ {
		for j := 0; j < size; j++ {
			s.localCBlock[i][j] += partialResult[i][j]
		}
	}
	s.debugLogf("Węzeł %s: addToLocalCBlock: C po dodaniu: %v", s.nodeName, s.localCBlock)
}

func (s *CannonServer) saveLocalCBlock() {
	err := os.MkdirAll("result", os.ModePerm)
	if err != nil {
		log.Fatalf("Węzeł %s: Nie można utworzyć katalogu 'result': %v", s.nodeName, err)
	}

	filename := fmt.Sprintf("result/result_block_%d_%d.txt", s.nodeRowIndex+1, s.nodeColIndex+1)
	file, err := os.Create(filename)
	if err != nil {
		log.Fatalf("Węzeł %s: Nie można utworzyć pliku %s: %v", s.nodeName, filename, err)
	}
	defer file.Close()

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	writer := bufio.NewWriter(file)
	for _, row := range s.localCBlock {
		for i, val := range row {
			fmt.Fprintf(writer, "%.2f", val)
			if i < len(row)-1 {
				fmt.Fprint(writer, " ")
			}
		}
		fmt.Fprintln(writer)
	}
	writer.Flush()

	s.debugLogf("Węzeł %s: Zapisano wynikowy blok macierzy C do pliku %s", s.nodeName, filename)
}

func (s *CannonServer) initializeLocalBlocks(nodeIndex int) ([][]float64, [][]float64) {
	aFile := fmt.Sprintf("matrices/A%d.txt", nodeIndex)
	bFile := fmt.Sprintf("matrices/B%d.txt", nodeIndex)

	s.debugLogf("Węzeł %d: Wczytywanie bloku A z pliku %s i B z pliku %s", nodeIndex, aFile, bFile)

	blockA := readSingleBlock(aFile)
	blockB := readSingleBlock(bFile)

	s.debugLogf("Węzeł %d: Blok A: %v", nodeIndex, blockA)
	s.debugLogf("Węzeł %d: Blok B: %v", nodeIndex, blockB)

	return blockA, blockB
}

func readSingleBlock(filename string) [][]float64 {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Nie można otworzyć pliku %s: %v", filename, err)
	}
	defer file.Close()

	var block [][]float64
	scanner := bufio.NewScanner(file)
	lineNumber := 0

	var rows int // Będziemy sprawdzać kwadratowość
	for scanner.Scan() {
		line := scanner.Text()
		lineNumber++
		trimmed := strings.TrimSpace(line)
		if len(trimmed) == 0 {
			// Dla pojedynczych bloków zakładamy brak pustych linii
			continue
		}
		numbers := strings.Fields(trimmed)
		if rows == 0 {
			rows = len(numbers) // Liczba kolumn w pierwszym wierszu
		}
		row := make([]float64, len(numbers))
		for i, numStr := range numbers {
			val, err := strconv.ParseFloat(numStr, 64)
			if err != nil {
				log.Fatalf("Nieprawidłowa liczba w pliku %s (linia %d): %v",
					filename, lineNumber, err)
			}
			row[i] = val
		}
		if len(row) != rows {
			log.Fatalf("W pliku %s, linia %d: oczekiwano %d kolumn, a jest %d",
				filename, lineNumber, rows, len(row))
		}
		block = append(block, row)
	}
	if err := scanner.Err(); err != nil {
		log.Fatalf("Błąd odczytu pliku %s: %v", filename, err)
	}

	// Sprawdzam kwadratowość
	if len(block) != rows {
		log.Fatalf("Blok w pliku %s nie jest kwadratowy: %d wierszy vs. %d kolumn",
			filename, len(block), rows)
	}

	return block
}

func getNeighbors(nodeRowIndex, nodeColIndex, totalNodesRows, totalNodesCols int) map[string]string {
	neighbors := make(map[string]string)

	directions := []struct {
		dRow, dCol int
		direction  string
	}{
		{-1, 0, "up"},
		{1, 0, "down"},
		{0, -1, "left"},
		{0, 1, "right"},
	}

	for _, dir := range directions {
		nRow := nodeRowIndex + dir.dRow
		nCol := nodeColIndex + dir.dCol

		// Zawijanie wierszy
		if nRow < 0 {
			nRow = totalNodesRows - 1
		}
		if nRow >= totalNodesRows {
			nRow = 0
		}
		// Zawijanie kolumn
		if nCol < 0 {
			nCol = totalNodesCols - 1
		}
		if nCol >= totalNodesCols {
			nCol = 0
		}

		neighborIndex := nRow*totalNodesCols + nCol
		neighborName := fmt.Sprintf("Node %d", neighborIndex+1)
		neighbors[dir.direction] = neighborName
	}

	return neighbors
}
