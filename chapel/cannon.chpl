use IO;
use Time;
use BlockDist;

// -----------------------------------------------------------------------------
// Parametry konfiguracyjne
// -----------------------------------------------------------------------------
config const N = 1800;          // Rozmiar macierzy (N x N)
config const debug = false;     // Debug macierzy podczas iteracji algorytmu
config const isLog = false;     // Sterowanie logami
config const isSave = false;    // Czy zapisać wynik do pliku

// -----------------------------------------------------------------------------
// Wyliczenie K na podstawie liczby węzłów i kontrola
// -----------------------------------------------------------------------------
proc computeKfromLocales(): int {
  const possibleK = sqrt(numLocales): int;
  if possibleK * possibleK != numLocales {
    halt("Błąd: liczba węzłów (", numLocales,
         ") nie jest kwadratem liczby naturalnej. Upewnij się, że numLocales jest kwadratem.");
  }
  return possibleK;
}

const K = computeKfromLocales();
if (N % K) != 0 {
  halt("Błąd: N=", N, " nie jest podzielne przez K=", K);
}
const blockSize = N / K;

// -----------------------------------------------------------------------------
// Utworzenie dystrybucji blokowej i domeny BlockDom
// -----------------------------------------------------------------------------
const dist = new blockDist({0..K-1, 0..K-1});
const FullDom: domain(2) dmapped dist = {1..N, 1..N};
var fullA: [FullDom] real;
var fullB: [FullDom] real;
const BlockDom: domain(2) dmapped dist = {0..K-1, 0..K-1};

// -----------------------------------------------------------------------------
// Deklaracja macierzy bloków - każda pozycja [i,j] to blok wielkości blockSize*blockSize
// -----------------------------------------------------------------------------
var aBlocks: [BlockDom] [0..blockSize-1, 0..blockSize-1] real;
var bBlocks: [BlockDom] [0..blockSize-1, 0..blockSize-1] real;
var cBlocks: [BlockDom] [0..blockSize-1, 0..blockSize-1] real; // domyślnie zerowe

// -----------------------------------------------------------------------------
// Łączenie bloków w jedną macierz NxN (wykonywane centralnie, tylko przy zapisie/debugu)
// -----------------------------------------------------------------------------
proc mergeBlocksIntoMatrix(
  blocks: [BlockDom] [0..blockSize-1, 0..blockSize-1] real
): [1..N, 1..N] real {
  var result: [1..N, 1..N] real;
  forall i in 0..K-1 {
    forall j in 0..K-1 {
      result[(i*blockSize+1) .. (i*blockSize+blockSize),
             (j*blockSize+1) .. (j*blockSize+blockSize)] =
        blocks[i,j];
    }
  }
  return result;
}

// -----------------------------------------------------------------------------
// Procedura zapisu całej macierzy do pliku
// -----------------------------------------------------------------------------
proc saveMatrixToFile(matrix: [1..N,1..N] real, filename: string) {
  var f = open(filename, ioMode.cw);
  var w = f.writer();
  for i in 1..N {
    w.writef("%0.1r", matrix[i,1]);
    for j in 2..N {
      w.writef(" %0.1r", matrix[i,j]);
    }
    w.writeln();
  }
  w.close();
  f.close();
}

// -----------------------------------------------------------------------------
// Wczytywanie pełnej macierzy NxN z pliku (wykonywane tylko na locale 0)
// Zwraca tablicę [1..N,1..N] real
// -----------------------------------------------------------------------------
proc readFullMatrix(filename: string): [1..N, 1..N] real {
  var full: [1..N, 1..N] real;
  var f = open(filename + "1.txt", ioMode.r);
  var reader = f.reader(locking=false);
  reader.read(full);
  reader.close();
  f.close();
  return full;
}

// -----------------------------------------------------------------------------
// Mnożenie bloków macierzy
// -----------------------------------------------------------------------------
proc multiplyBlock(
  ref A: [0..blockSize-1, 0..blockSize-1] real,
  ref B: [0..blockSize-1, 0..blockSize-1] real,
  ref C: [0..blockSize-1, 0..blockSize-1] real
) {
  forall i in 0..blockSize-1 {
    for k in 0..blockSize-1 {
      const aik = A[i,k];
      for j in 0..blockSize-1 {
        C[i,j] += aik * B[k,j];
      }
    }
  }
}

// proc multiplyBlock(
//   ref A: [0..blockSize-1, 0..blockSize-1] real,
//   ref B: [0..blockSize-1, 0..blockSize-1] real,
//   ref C: [0..blockSize-1, 0..blockSize-1] real)
//   {
//   forall i in 0..blockSize-1 {
//     for j in 0..blockSize-1 {
//       var sum: real = 0;
//       for k in 0..blockSize-1 {
//         sum += A[i,k] * B[k,j];
//       }
//       C[i,j] += sum;
//     }
//   }
// }

// -----------------------------------------------------------------------------
// Pomocnicze funkcje do logowania
// -----------------------------------------------------------------------------
proc debugPrintMatrix(title: string, mat: [1..N, 1..N] real) {
  if debug {
    writeln(title, ":");
    for i in 1..N {
      write("[");
      for j in 1..N {
        write(mat[i,j], " ");
      }
      writeln("]");
    }
    writeln();
  }
}

proc printLog(msg: string) {
  if isLog then writeln(msg);
}

// -----------------------------------------------------------------------------
// Kolektywna operacja przesunięcia bloków
// -----------------------------------------------------------------------------
proc collectiveShiftOptimized() {
  var tempA: [BlockDom] [0..blockSize-1, 0..blockSize-1] real;
  var tempB: [BlockDom] [0..blockSize-1, 0..blockSize-1] real;

  // Przesunięcie bloków aBlocks - wiersze cyklicznie w lewo o 1
  forall (i,j) in BlockDom do
    on tempA[i,j] {
      tempA[i,j] = aBlocks[i, (j + 1) % K];
    }

  // Przesunięcie bloków bBlocks - kolumny cyklicznie w górę o 1
  forall (i,j) in BlockDom do
    on tempB[i,j] {
      tempB[i,j] = bBlocks[(i + 1) % K, j];
    }

  aBlocks = tempA;
  bBlocks = tempB;
}

// -----------------------------------------------------------------------------
// Główna procedura
// -----------------------------------------------------------------------------
proc main() {
  writeln(
    ">>> Start programu: N=", N,
    ", K=", K,
    ", blockSize=", blockSize,
    ", numLocales=", numLocales
  );

  // Stopery do pomiaru czasu
  var swTotal = new stopwatch();    // Całkowity czas wykonania programu
  var swCannon = new stopwatch();   // Całkowity czas algorytmu Cannona
  var swAlign = new stopwatch();    // Czas wstępnego przesunięcia
  var swIter = new stopwatch();     // Czas iteracji (mnożenie + przesunięcie cykliczne)
  var swShift = new stopwatch();    // Czas samego przesunięcia bloków
  var swLoad = new stopwatch();     // Czas wczytywania macierzy z plików

  // Start głównego timera
  swTotal.start();

  if here.id == 0 then
    writeln(">>> Locale 0 wczytuje pełne macierze A i B ...");

  var fullA: [1..N, 1..N] real;
  var fullB: [1..N, 1..N] real;

  // Węzeł 0 wczytuje A i B równolegle
  swLoad.start();
  sync {
    begin {
      fullA = readFullMatrix("matrices/A");
    }
    begin {
      fullB = readFullMatrix("matrices/B");
    }
  }
  swLoad.stop();
  writeln(">>> Czas wczytania pełnych macierzy A i B = ",
          swLoad.elapsed():string, " s.");

  // Przepisanie dystrybuowanych macierzy do lokalnych bloków
  forall (i,j) in BlockDom do
    on aBlocks[i,j] {
      const rowRange = i*blockSize+1 .. i*blockSize+blockSize;
      const colRange = j*blockSize+1 .. j*blockSize+blockSize;
      aBlocks[i,j] = fullA[rowRange, colRange];
    }
  forall (i,j) in BlockDom do
    on bBlocks[i,j] {
      const rowRange = i*blockSize+1 .. i*blockSize+blockSize;
      const colRange = j*blockSize+1 .. j*blockSize+blockSize;
      bBlocks[i,j] = fullB[rowRange, colRange];
    }

  if here.id == 0 then
    printLog(">>> Rozpoczynam wstępne przesunięcie bloków ...");

  // Start algorytmu Cannona
  swCannon.start();
  swAlign.start();

  var tempAinit: [BlockDom] [0..blockSize-1, 0..blockSize-1] real;
  var tempBinit: [BlockDom] [0..blockSize-1, 0..blockSize-1] real;

  // Wstępne przesunięcie wg algorytmu Cannona
  forall (i,j) in BlockDom do
    on tempAinit[i,j] {
      tempAinit[i,j] = aBlocks[i, (j + i) % K];
    }

  forall (i,j) in BlockDom do
    on tempBinit[i,j] {
      tempBinit[i,j] = bBlocks[(i + j) % K, j];
    }

  aBlocks = tempAinit;
  bBlocks = tempBinit;

  swAlign.stop();
  if here.id == 0 then
    printLog(">>> Zakończono wstępne przesunięcie => Czas = "
            + swAlign.elapsed():string + " s.");

  // Główna pętla iteracyjna
  for iteration in 0..K-1 {
    if here.id == 0 then
      printLog(">>> Iteracja " + iteration:string + " rozpoczęta ...");
    swIter.start();

    // Mnożenie bloków
    forall (i,j) in BlockDom with (ref aBlocks, ref bBlocks, ref cBlocks) do
      on aBlocks[i,j] {
        var localMultTimer = new stopwatch();
        localMultTimer.start();

        var localA = aBlocks[i,j];
        var localB = bBlocks[i,j];
        var localC = cBlocks[i,j];

        multiplyBlock(localA, localB, localC);

        localMultTimer.stop();
        cBlocks[i,j] = localC;
        printLog("Locale #" + here.id:string
                + " | Iteracja " + iteration:string
                + " | Mnożenie bloku [" + i:string
                + "," + j:string + "] => "
                + localMultTimer.elapsed():string + " s.");
      }

    // Kolektywne przesunięcie bloków
    if here.id == 0 then
      printLog("  >> Iteracja " + iteration:string + " => zaczynam przesunięcie bloków ...");
    swShift.start();

    collectiveShiftOptimized();

    swShift.stop();
    if here.id == 0 then
      printLog(
        "  >> Iteracja " + iteration:string
        + " => przesunięcie zakończone => " + swShift.elapsed():string + " s."
      );

    swIter.stop();
    if here.id == 0 then
      printLog(
        ">>> Iteracja " + iteration:string + " zakończona => Całkowity czas (wszystkich) iteracji = "
        + swIter.elapsed():string + " s."
      );

    // Pomocniczy debug
    if debug && here.id == 0 {
      const tmpC = mergeBlocksIntoMatrix(cBlocks);
      debugPrintMatrix("Macierz C po iteracji " + iteration:string, tmpC);
    }
  }

  // Zakończenie obliczeń i zapis wyniku (jeśli isSave=true)
  swCannon.stop();
  swTotal.stop();
  if here.id == 0 then
    writeln(">>> Zakończono mnożenie (bez zapisu) => Czas algorytmu = ",
            swCannon.elapsed():string + " s.");

  if here.id == 0 && isSave {
    writeln(">>> Zapisuję wynik do pliku ...");
    var swSave = new stopwatch();
    swSave.start();
    const mergedC = mergeBlocksIntoMatrix(cBlocks);
    saveMatrixToFile(mergedC, "result/full_result_matrix.txt");
    swSave.stop();
    writeln(">>> Zapisano wynik => Czas zapisu = ", swSave.elapsed():string + " s.");
    writeln(
      ">>> Koniec programu => Łączny czas (I/O + algorytm) = ",
      (swTotal.elapsed() + swSave.elapsed()):string + " s."
    );
  } else if here.id == 0 {
    writeln(">>> Koniec programu => Łączny czas (I/O + algorytm) = ", swTotal.elapsed():string + " s.");
  }
}
