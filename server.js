const express = require("express")
const http = require("http")
const WebSocket = require("ws")

const app = express()
const server = http.createServer(app)
const wss = new WebSocket.Server({ server })

const MAX_BOTS_PER_WORKER = 3

app.use(express.static("public"))

// Map<ws, string[]> which bots each worker owns
const workerBots = new Map()

// Map<string, ws> which worker owns each bot username
const botWorker = new Map()

function getAvailableWorker() {
    for (const [worker, bots] of workerBots.entries()) {
        if (worker.readyState === WebSocket.OPEN && bots.length < MAX_BOTS_PER_WORKER) {
            return worker
        }
    }
    return null
}

function broadcastBotList() {
    const bots = Array.from(botWorker.keys())
    const msg = JSON.stringify({ type: "botList", bots })

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
                workerBots.set(ws, [])
                console.log("worker connected, total workers:", workerBots.size)
            } else if (data.role === "panel") {
                ws.role = "panel"
                // Send current bot list to newly connected panel
                ws.send(JSON.stringify({
                    type: "botList",
                    bots: Array.from(botWorker.keys())
                }))
            }
            return
        }

        // --- Worker reporting its current bot list ---
        // Workers send { type: "botList", bots: [...] } after each spawn/delete
        if (ws.role === "worker" && data.type === "botList") {
            const previousBots = workerBots.get(ws) || []
            const newBots = data.bots

            // Remove bots this worker no longer has
            for (const username of previousBots) {
                if (!newBots.includes(username)) {
                    botWorker.delete(username)
                }
            }

            // Register bots this worker now has
            for (const username of newBots) {
                botWorker.set(username, ws)
            }

            workerBots.set(ws, newBots)
            broadcastBotList()
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
                    // Send delete to every worker for every bot they own
                    for (const [worker, bots] of workerBots.entries()) {
                        for (const username of [...bots]) {
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
                    for (const worker of workerBots.keys()) {
                        sendToWorker(worker, cmd)
                    }
                    return
                }

                const worker = botWorker.get(cmd.username)
                if (!worker) return
                sendToWorker(worker, cmd)
                return
            }

            if (cmd.type === "dropAll") {
                if (cmd.username === "__all__") {
                    for (const worker of workerBots.keys()) {
                        sendToWorker(worker, cmd)
                    }
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

        const lostBots = workerBots.get(ws) || []
        workerBots.delete(ws)

        console.log(`worker disconnected, lost bots: [${lostBots.join(", ")}]`)

        // Remove from botWorker map first
        for (const username of lostBots) {
            botWorker.delete(username)
        }

        // Reassign lost bots to available workers
        for (const username of lostBots) {
            const worker = getAvailableWorker()
            if (!worker) {
                console.log(`no available worker to reassign bot: ${username}`)
                continue
            }

            sendToWorker(worker, { type: "createBot", username })

            // Optimistically track the assignment; the worker will confirm via botList
            const workerList = workerBots.get(worker)
            workerList.push(username)
            botWorker.set(username, worker)
        }

        broadcastBotList()
    })
})

server.listen(3000, () => {
    console.log("http://localhost:3000")
})