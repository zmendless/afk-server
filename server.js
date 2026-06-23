const express = require("express")
const http = require("http")
const WebSocket = require("ws")

const app = express()
const server = http.createServer(app)
const wss = new WebSocket.Server({ server })

const MAX_BOTS_PER_WORKER = 3

app.use(express.static("public"))

// Map<ws, { id, confirmed: string[], pending: string[] }>
const workerBots = new Map()

// Map<string, ws>
const botWorker = new Map()

let workerIdCounter = 1

function getAvailableWorker() {
    let best = null
    let fewest = MAX_BOTS_PER_WORKER

    for (const [worker, state] of workerBots.entries()) {
        const total = state.confirmed.length + state.pending.length
        if (worker.readyState === WebSocket.OPEN && total < fewest) {
            best = worker
            fewest = total
        }
    }

    return best
}

function broadcastToPanel(msg) {
    wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN && client.role === "panel") {
            client.send(msg)
        }
    })
}

function broadcastBotList() {
    const bots = Array.from(botWorker.keys())
    broadcastToPanel(JSON.stringify({ type: "botList", bots }))
}

function broadcastWorkerList() {
    const workers = Array.from(workerBots.values()).map(s => ({
        id: s.id,
        bots: s.confirmed.length + s.pending.length
    }))
    broadcastToPanel(JSON.stringify({ type: "workerList", workers }))
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
                workerBots.set(ws, { id: ws.workerId, confirmed: [], pending: [] })
                console.log(`worker #${ws.workerId} connected, total: ${workerBots.size}`)
                broadcastWorkerList()
            } else if (data.role === "panel") {
                ws.role = "panel"
                ws.send(JSON.stringify({ type: "botList", bots: Array.from(botWorker.keys()) }))
                ws.send(JSON.stringify({
                    type: "workerList",
                    workers: Array.from(workerBots.values()).map(s => ({
                        id: s.id,
                        bots: s.confirmed.length + s.pending.length
                    }))
                }))
            }
            return
        }

        // --- Worker reporting its bot list ---
        if (ws.role === "worker" && data.type === "botList") {
            const state = workerBots.get(ws)
            if (!state) return

            const reportedBots = data.bots

            // Move pending → confirmed if worker now has them
            state.pending = state.pending.filter(username => {
                if (reportedBots.includes(username)) {
                    state.confirmed.push(username)
                    return false // remove from pending
                }
                return true // keep in pending, not spawned yet
            })

            // Remove confirmed bots the worker no longer has
            state.confirmed = state.confirmed.filter(username => {
                if (!reportedBots.includes(username)) {
                    botWorker.delete(username)
                    return false
                }
                return true
            })

            // Add any bots the worker has that we don't know about (e.g. after reassignment)
            for (const username of reportedBots) {
                if (!state.confirmed.includes(username) && !state.pending.includes(username)) {
                    state.confirmed.push(username)
                    botWorker.set(username, ws)
                }
            }

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
                const state = workerBots.get(worker)
                state.pending.push(cmd.username)
                botWorker.set(cmd.username, worker)
                sendToWorker(worker, cmd)
                broadcastWorkerList()
                return
            }

            if (cmd.type === "deleteBot") {
                if (cmd.username === "__all__") {
                    for (const [worker, state] of workerBots.entries()) {
                        const all = [...state.confirmed, ...state.pending]
                        for (const username of all) {
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
        const lostBots = state ? [...state.confirmed, ...state.pending] : []
        workerBots.delete(ws)

        console.log(`worker #${ws.workerId} disconnected, lost bots: [${lostBots.join(", ")}]`)

        for (const username of lostBots) botWorker.delete(username)

        for (const username of lostBots) {
            const worker = getAvailableWorker()
            if (!worker) {
                console.log(`no available worker to reassign bot: ${username}`)
                continue
            }
            const workerState = workerBots.get(worker)
            workerState.pending.push(username)
            botWorker.set(username, worker)
            sendToWorker(worker, { type: "createBot", username })
        }

        broadcastBotList()
        broadcastWorkerList()
    })
})

const PORT = process.env.PORT || 3000
server.listen(PORT, () => console.log(`listening on port ${PORT}`))
