const express = require("express")
const http = require("http")
const WebSocket = require("ws")

const app = express()
const server = http.createServer(app)
const wss = new WebSocket.Server({ server })

const MAX_BOTS_PER_WORKER = 3

app.use(express.static("public"))

const workerBots = new Map() // Map<ws, { id, confirmed: string[], pending: string[] }>
const botWorker = new Map() // Map<string, ws>
let workerIdCounter = 1

function getAvailableWorker() {
    let best = null
    let most = -1
    for (const [worker, state] of workerBots.entries()) {
        const total = state.confirmed.length + state.pending.length
        if (worker.readyState === WebSocket.OPEN && total < MAX_BOTS_PER_WORKER && total > most) {
            best = worker
            most = total
        }
    }
    return best
}

function broadcastToPanel(msg) {
    wss.clients.forEach(c => {
        if (c.readyState === WebSocket.OPEN && c.role === "panel") c.send(msg)
    })
}

function broadcastBotList() {
    broadcastToPanel(JSON.stringify({ type: "botList", bots: Array.from(botWorker.keys()) }))
}

function broadcastWorkerList() {
    const workers = Array.from(workerBots.values()).map(s => ({
        id: s.id,
        bots: s.confirmed.length + s.pending.length,
        names: s.confirmed
    }))
    broadcastToPanel(JSON.stringify({ type: "workerList", workers }))
}

function sendToWorker(worker, payload) {
    if (worker.readyState === WebSocket.OPEN) worker.send(JSON.stringify(payload))
}

wss.on("connection", (ws) => {
    ws.role = "unknown"

    ws.on("message", (msg) => {
        const data = JSON.parse(msg)

        if (data.type === "register") {
            if (data.role === "bot-worker") {
                ws.role = "worker"
                ws.workerId = workerIdCounter++
                workerBots.set(ws, { id: ws.workerId, confirmed: [], pending: [] })
                console.log(`Worker #${ws.workerId} connected`)
                broadcastWorkerList()
            } else if (data.role === "panel") {
                ws.role = "panel"
                ws.send(JSON.stringify({ type: "botList", bots: Array.from(botWorker.keys()) }))
                ws.send(JSON.stringify({ type: "workerList", workers: Array.from(workerBots.values()).map(s => ({ id: s.id, bots: s.confirmed.length + s.pending.length, names: s.confirmed })) }))
            }
            return
        }

        if (ws.role === "worker" && data.type === "botList") {
            const state = workerBots.get(ws)
            if (!state) return

            const reported = data.bots

            // pending → confirmed
            state.pending = state.pending.filter(u => {
                if (reported.includes(u)) { state.confirmed.push(u); return false }
                return true
            })

            // remove lost confirmed bots
            state.confirmed = state.confirmed.filter(u => {
                if (!reported.includes(u)) { botWorker.delete(u); return false }
                return true
            })

            // adopt unknown bots or kill duplicates
            for (const u of reported) {
                if (!state.confirmed.includes(u) && !state.pending.includes(u)) {
                    if (!botWorker.has(u)) {
                        state.confirmed.push(u)
                        botWorker.set(u, ws)
                    } else {
                        sendToWorker(ws, { type: "deleteBot", username: u })
                    }
                }
            }

            broadcastBotList()
            broadcastWorkerList()
            return
        }

        if (data.type === "command") {
            const cmd = data.payload

            if (cmd.type === "createBot") {
                if (botWorker.has(cmd.username)) return
                const worker = getAvailableWorker()
                if (!worker) { console.log(`No worker available for ${cmd.username}`); return }
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
                        for (const u of [...state.confirmed, ...state.pending]) {
                            sendToWorker(worker, { type: "deleteBot", username: u })
                            botWorker.delete(u)
                        }
                        state.confirmed = []
                        state.pending = []
                    }
                    broadcastBotList()
                    broadcastWorkerList()
                    return
                }
                const worker = botWorker.get(cmd.username)
                if (!worker) return
                sendToWorker(worker, cmd)
                botWorker.delete(cmd.username)
                const state = workerBots.get(worker)
                if (state) {
                    state.confirmed = state.confirmed.filter(u => u !== cmd.username)
                    state.pending = state.pending.filter(u => u !== cmd.username)
                }
                broadcastBotList()
                broadcastWorkerList()
                return
            }

            if (cmd.type === "sendMessage") {
                if (cmd.username === "__all__") { for (const w of workerBots.keys()) sendToWorker(w, cmd); return }
                const worker = botWorker.get(cmd.username)
                if (worker) sendToWorker(worker, cmd)
                return
            }

            if (cmd.type === "dropAll") {
                if (cmd.username === "__all__") { for (const w of workerBots.keys()) sendToWorker(w, cmd); return }
                const worker = botWorker.get(cmd.username)
                if (worker) sendToWorker(worker, cmd)
                return
            }
        }
    })

    ws.on("close", () => {
        if (ws.role !== "worker") return
        const state = workerBots.get(ws)
        const lost = state ? [...state.confirmed, ...state.pending] : []
        workerBots.delete(ws)
        console.log(`Worker #${ws.workerId} disconnected, lost: [${lost.join(", ") || "none"}]`)
        for (const u of lost) botWorker.delete(u)
        for (const u of lost) {
            const worker = getAvailableWorker()
            if (!worker) { console.log(`No worker to reassign ${u}`); continue }
            const s = workerBots.get(worker)
            s.pending.push(u)
            botWorker.set(u, worker)
            sendToWorker(worker, { type: "createBot", username: u })
        }
        broadcastBotList()
        broadcastWorkerList()
    })
})

const PORT = process.env.PORT || 3000
server.listen(PORT, () => console.log(`listening on port ${PORT}`))