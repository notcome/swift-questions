import Foundation

@main
struct App {
    static func main() async throws {
        await (0..<100).parallelForEach { i in
            let count = (200..<1000).randomElement()!

            let state = CombineResultState()
            Task.detached {
                let isBomb = [true, false].randomElement()!
                try! await Task.sleep(nanoseconds: 300_000_000)
                let bomb: Bomb? = isBomb ? Bomb() : nil
                await state.write(failure: bomb)
            }

            do {
                var sum = 0
                for await i in count.boringSequence() {
                    try await state.checkError()
                    sum += i
                }
                try await state.waitForCompletion()
                print(i, sum)
            } catch {
                print(i, "Bomb")
            }
        }
    }
}

let logLock = NSLock()

var signalCount = 0
var counterCount = 0

func logSignal() {
    logLock.lock()
    signalCount += 1
    print("Signal exited \(signalCount)")
    logLock.unlock()
}

func logCounter() {
    logLock.lock()
    counterCount += 1
    print("Before counter \(counterCount)")
    logLock.unlock()
}

public actor Semaphore {
    public private(set) var permits: Int
    private var queue: [CheckedContinuation<Void, Never>] = []

    public init(initialPermits: Int = 0) {
        permits = initialPermits
    }

    public func signal() {
        defer { logSignal() }
        permits += 1
        guard permits > 0, !queue.isEmpty else { return }
        assert(permits == 1)

        permits -= 1
        let cont = queue.first!
        queue.removeFirst()
        cont.resume()
    }

    public func wait() async {
        guard permits <= 0 else {
            permits -= 1
            return
        }

        await withCheckedContinuation { cont in
            queue.append(cont)
        }
    }
}

public actor Group {
    private var counter: Int = 0
    private var queue: [CheckedContinuation<Void, Never>] = []

    public func enter() {
        counter += 1
        clearQueue()
    }

    public func leave() {
        counter -= 1
        clearQueue()
    }

    private func clearQueue() {
        guard counter == 0 else { return }
        let queue = self.queue
        self.queue = []
        for cont in queue {
            cont.resume()
        }
    }

    public func wait() async {
        await withCheckedContinuation { cont in
            queue.append(cont)
        }
    }
}

extension Sequence where Element: Sendable {
    public func parallelForEach(concurrentCount: Int = ProcessInfo.processInfo.activeProcessorCount, body: @escaping @Sendable (Element) async -> Void) async {
        assert(concurrentCount >= 1)
        let counter = Group()
        let limiter = Semaphore(initialPermits: concurrentCount)

        for element in self {
            await limiter.wait()
            Task.detached {
                await counter.enter()
                await body(element)
                await limiter.signal()
                logCounter()
                await counter.leave()
            }
        }

        await counter.wait()
    }
}

private actor CombineResultState {
    private var result: Result<Void, Error>?
    private var cont: CheckedContinuation<Void, Error>?

    func write(failure: Error?) {
        let result: Result<Void, Error>
        if let failure = failure {
            result = .failure(failure)
        } else {
            result = .success(())
        }
        if let cont = cont {
            cont.resume(with: result)
        } else {
            self.result = result
        }
    }

    func checkError() throws {
        if case .failure(let error) = result {
            throw error
        }
    }

    func waitForCompletion() async throws {
        guard result == nil else {
            try checkError()
            return
        }

        try await withCheckedThrowingContinuation {
            self.cont = $0
        }
    }
}

extension Int {
    func boringSequence() -> AsyncStream<Int> {
        var i: Int = 0
        let max = self
        return AsyncStream {
            try! await Task.sleep(nanoseconds: 1000_000)
            i += 1
            if i > max {
                return nil
            }
            return i
        }

    }
}

struct Bomb: Error {}
