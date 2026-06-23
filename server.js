const express = require("express")
const http = require("http")
const WebSocket = require("ws")

const app = express()
const server = http.createServer(app)
const wss = new WebSocket.Server({ server })

const MAX_BOTS_PER_WORKER = 3

app.use(express.static("public"))

// Map<ws, { id, bots[] }> — worker state
const workerBots = new Map()

// Map<string, ws> — which worker owns each bot username
const botWorker = new Map()

let workerIdCounter = 1

function getAvailableWorker() {
    let best = null
    let fewest = MAX_BOTS_PER_WORKER

    for (const [worker, state] of workerBots.entries()) {
        if (worker.readyState === WebSocket.OPEN && state.bots.length < fewest) {
            best = worker
            fewest = state.bots.length
        }
    }

    return best
}

function broadcastBotList() {
    const bots = Array.from(botWorker.keys())
    const msg = JSON.stringify({ type: "botList", bots })
    broadcastToPanel(msg)
}

function broadcastWorkerList() {
    const workers = Array.from(workerBots.values()).map(state => ({
        id: state.id,
        bots: state.bots
    }))
    broadcastToPanel(JSON.stringify({ type: "workerList", workers }))
}

function broadcastToPanel(msg) {
    wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN && client.role === "panel") {
            client.send(msg)
        }
    })
}

function sendToWorker(worker, payload) {
    if (worker.readyState === WebSocket.OPEN) {
        worker.send(JSON.stringify(payload))
    }
}

wss.on("connection", (ws) => {
    ws.role = "unknown"

    ws.on("message", (msg) => {
        const data = JSON.parse(msg)

        // --- Registration ---
        if (data.type === "register") {
            if (data.role === "bot-worker") {
                ws.role = "worker"
                ws.workerId = workerIdCounter++
                workerBots.set(ws, { id: ws.workerId, bots: [] })
                console.log(`worker #${ws.workerId} connected, total: ${workerBots.size}`)
                broadcastWorkerList()
            } else if (data.role === "panel") {
                ws.role = "panel"
                ws.send(JSON.stringify({ type: "botList", bots: Array.from(botWorker.keys()) }))
                ws.send(JSON.stringify({
                    type: "workerList",
                    workers: Array.from(workerBots.values()).map(s => ({ id: s.id, bots: s.bots }))
                }))
            }
            return
        }

        // --- Worker reporting its current bot list ---
        if (ws.role === "worker" && data.type === "botList") {
            const state = workerBots.get(ws)
            const previousBots = state ? state.bots : []
            const newBots = data.bots

            for (const username of previousBots) {
                if (!newBots.includes(username)) botWorker.delete(username)
            }
            for (const username of newBots) {
                botWorker.set(username, ws)
            }

            if (state) state.bots = newBots
            broadcastBotList()
            broadcastWorkerList()
            return
        }

        // --- Panel commands ---
        if (data.type === "command") {
            const cmd = data.payload

            if (cmd.type === "createBot") {
                const worker = getAvailableWorker()
                if (!worker) {
                    console.log("all workers at capacity, rejecting createBot for:", cmd.username)
                    return
                }
                sendToWorker(worker, cmd)
                return
            }

            if (cmd.type === "deleteBot") {
                if (cmd.username === "__all__") {
                    for (const [worker, state] of workerBots.entries()) {
                        for (const username of [...state.bots]) {
                            sendToWorker(worker, { type: "deleteBot", username })
                        }
                    }
                    return
                }
                const worker = botWorker.get(cmd.username)
                if (!worker) return
                sendToWorker(worker, cmd)
                return
            }

            if (cmd.type === "sendMessage") {
                if (cmd.username === "__all__") {
                    for (const worker of workerBots.keys()) sendToWorker(worker, cmd)
                    return
                }
                const worker = botWorker.get(cmd.username)
                if (!worker) return
                sendToWorker(worker, cmd)
                return
            }

            if (cmd.type === "dropAll") {
                if (cmd.username === "__all__") {
                    for (const worker of workerBots.keys()) sendToWorker(worker, cmd)
                    return
                }
                const worker = botWorker.get(cmd.username)
                if (!worker) return
                sendToWorker(worker, cmd)
                return
            }
        }
    })

    ws.on("close", () => {
        if (ws.role !== "worker") return

        const state = workerBots.get(ws)
        const lostBots = state ? state.bots : []
        workerBots.delete(ws)

        console.log(`worker #${ws.workerId} disconnected, lost bots: [${lostBots.join(", ")}]`)

        for (const username of lostBots) botWorker.delete(username)

        for (const username of lostBots) {
            const worker = getAvailableWorker()
            if (!worker) {
                console.log(`no available worker to reassign bot: ${username}`)
                continue
            }
            sendToWorker(worker, { type: "createBot", username })
            const workerState = workerBots.get(worker)
            workerState.bots.push(username)
            botWorker.set(username, worker)
        }

        broadcastBotList()
        broadcastWorkerList()
    })
})

const PORT = process.env.PORT || 3000
server.listen(PORT, () => console.log(`http://localhost:${PORT}`))
