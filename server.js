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
        bots: s.confirmed.length + s.pending.length,
        names: s.confirmed
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
                console.log(`[+] Worker #${ws.workerId} connected, total workers: ${workerBots.size}`)
                broadcastWorkerList()
            } else if (data.role === "panel") {
                ws.role = "panel"
                ws.send(JSON.stringify({ type: "botList", bots: Array.from(botWorker.keys()) }))
                ws.send(JSON.stringify({
                    type: "workerList",
                    workers: Array.from(workerBots.values()).map(s => ({
                        id: s.id,
                        bots: s.confirmed.length + s.pending.length,
                        names: s.confirmed
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
            console.log(`[Worker #${ws.workerId}] reported bots: [${reportedBots.join(", ") || "none"}]`)

            // Move pending → confirmed if worker now has them
            state.pending = state.pending.filter(username => {
                if (reportedBots.includes(username)) {
                    console.log(`[Worker #${ws.workerId}] ${username} confirmed`)
                    state.confirmed.push(username)
                    return false
                }
                return true
            })

            // Remove confirmed bots the worker no longer has
            state.confirmed = state.confirmed.filter(username => {
                if (!reportedBots.includes(username)) {
                    console.log(`[Worker #${ws.workerId}] ${username} lost`)
                    botWorker.delete(username)
                    return false
                }
                return true
            })

            // Add any bots the worker has that we don't know about (e.g. after reconnect)
            for (const username of reportedBots) {
                if (!state.confirmed.includes(username) && !state.pending.includes(username)) {
                    console.log(`[Worker #${ws.workerId}] ${username} adopted (unknown bot)`)
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
                    console.log(`[!] All workers at capacity, rejecting createBot for: ${cmd.username}`)
                    return
                }
                const state = workerBots.get(worker)
                state.pending.push(cmd.username)
                botWorker.set(cmd.username, worker)
                sendToWorker(worker, cmd)
                broadcastWorkerList()
                console.log(`[Worker #${worker.workerId}] assigned bot: ${cmd.username} (pending)`)

                // Expire pending after 60s if worker never confirms
                setTimeout(() => {
                    const s = workerBots.get(worker)
                    if (!s) return
                    const idx = s.pending.indexOf(cmd.username)
                    if (idx !== -1) {
                        console.log(`[!] Pending bot ${cmd.username} never confirmed after 60s, expiring`)
                        s.pending.splice(idx, 1)
                        botWorker.delete(cmd.username)
                        broadcastWorkerList()
                        broadcastBotList()
                    }
                }, 60000)

                return
            }

            if (cmd.type === "deleteBot") {
                if (cmd.username === "__all__") {
                    console.log(`[Panel] delete all bots`)
                    for (const [worker, state] of workerBots.entries()) {
                        const all = [...state.confirmed, ...state.pending]
                        for (const username of all) {
                            sendToWorker(worker, { type: "deleteBot", username })
                            botWorker.delete(username)
                        }
                        state.confirmed = []
                        state.pending = []
                    }
                    broadcastBotList()
                    broadcastWorkerList()
                    return
                }
                const worker = botWorker.get(cmd.username)
                if (!worker) {
                    console.log(`[!] deleteBot: no worker found for ${cmd.username}`)
                    return
                }
                sendToWorker(worker, cmd)
                botWorker.delete(cmd.username)
                const state = workerBots.get(worker)
                if (state) {
                    state.confirmed = state.confirmed.filter(u => u !== cmd.username)
                    state.pending = state.pending.filter(u => u !== cmd.username)
                }
                console.log(`[Worker #${worker.workerId}] deleted bot: ${cmd.username}`)
                broadcastWorkerList()
                broadcastBotList()
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

        console.log(`[-] Worker #${ws.workerId} disconnected. Lost bots: [${lostBots.join(", ") || "none"}]`)

        for (const username of lostBots) botWorker.delete(username)

        for (const username of lostBots) {
            const worker = getAvailableWorker()
            if (!worker) {
                console.log(`[!] No available worker to reassign bot: ${username}`)
                continue
            }
            const workerState = workerBots.get(worker)
            workerState.pending.push(username)
            botWorker.set(username, worker)
            sendToWorker(worker, { type: "createBot", username })
            console.log(`[Worker #${worker.workerId}] reassigned bot: ${username}`)
        }

        broadcastBotList()
        broadcastWorkerList()
    })
})

const PORT = process.env.PORT || 3000
server.listen(PORT, () => console.log(`listening on port ${PORT}`))
