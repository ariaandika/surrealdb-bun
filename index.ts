export class SurrealError extends Error {
    constructor(public message: string, public detail: any) {
        super(message)
    }
}

export class Surreal {
    /**
    * providing ns and db will automatically call `use` it
    *
    * @example
    * import { Surreal, SurrealError } from "surreal-bun"
    * const db = new Surreal("ws://127.0.0.1:8000/rpc",{ ns: "test", db: "test" });
    *
    * try {
    *     console.log(await db.create("app",{ my: 412 }))
    *     console.log(await db.select("app"))
    * } catch (err) {
    *     console.error(err instanceof SurrealError,err)
    * } finally {
    *     db.close()
    * }
    */
    constructor(
        public url: string,
        public opt?: Partial<{
            ns: string
            db: string
            log: boolean
        }>,
    ) {
        if (opt) {
            this.cred = { db: opt.db, ns: opt.ns }
        }
    }

    cred?: Partial<{ ns: string, db: string }>
    ws!: WebSocket
    state = 0
    channels = {} as Record<number,{
        resolve: CallableFunction, reject: CallableFunction, sql: string
    }>

    private get id() {
        if (this.state > 1000) {
            this.state = 0
        }
        return this.state++
    }

    private async createWebSocket() {
        this.opt?.log && console.log(`[Surreal] new WebSocket ${this.url}`)
        const { resolve, reject, promise } = Promise.withResolvers<WebSocket>()

        const ws = new WebSocket(this.url)

        const onerror = (e: any) => reject(new SurrealError(e.message,e))
        const onclose = (e: any) => reject(new SurrealError(e.reason,e))
        ws.addEventListener("close",onclose)
        ws.addEventListener("error",onerror)
        ws.addEventListener("open", () => {
            ws.removeEventListener("close",onclose)
            ws.removeEventListener("error",onerror)
            resolve(ws)
        }, { once: true })

        return promise
    }

    private async setup() {
        if (this.ws && this.ws.readyState === this.ws.OPEN) {
            return
        }

        this.ws = await this.createWebSocket()

        /** return unmodified response */
        this.ws.addEventListener("message", e => {
            try {
                const app = JSON.parse(e.data.toString()) as { id: number, result: object }
                const channel = this.channels[app.id]

                // TODO: handle live query
                if (!channel) {
                    console.warn(`[WebSocket] No channel present for id ${app.id}`)
                    return
                }

                try {
                    channel.resolve(app)
                } catch (err) {
                    channel.reject(err)
                } finally {
                    delete this.channels[app.id]
                }
            } catch (err) {
                throw new SurrealError("Failed parse JSON from surreal: " + e.data, err)
            }
        })

        if (this.opt?.ns && this.opt.db) {
            await this.use(this.opt.ns,this.opt.db)
        }
    }

    close() { return this.ws?.close() }

    async send(method: string, params?: string) {
        await this.setup()

        const id = this.id
        const { promise, resolve, reject } = Promise.withResolvers<Record<string,any>>()
        const sql = `{"id":${id},"method":"${method}"${
            params ? `,"params":${params}` : ""
        }}`

        this.channels[id] = { resolve, reject, sql }
        this.ws.send(sql)

        const result = await promise

        if (result.error) {
            result.error.sql = sql
            throw new SurrealError(result.error.message,result.error)
        }

        return result.result
    }

    use(ns: string, db: string) {
        return this.send("use",`["${ns}","${db}"]`)
    }

    info() {
        return this.send("info")
    }

    signup({
        ns, db, sc, username, password
    }:{
        ns: string, db: string, sc: string,
        username: string, password: string,
    }): Promise<string> {
        return this.send(
            "signup",
            `[{"NS":"${ns}","DB":"${db}","SC":"${
                sc
            }","username":"${username}","password":"${password}"}]`
        )
    }

    /** to signin as root, only provide user and pass */
    signin({
        ns, db, sc, user, pass
    }:{
        ns?: string, db?: string, sc?: string,
        user: string, pass: string
    }) {
        return this.send(
            "signin",
            `[{"user":"${user}","pass":"${
                pass
            }"${
                (ns ? `,"NS":"${ns}"` : "") +
                (db ? `,"DB":"${db}"` : "") +
                (sc ? `,"SC":"${sc}"` : "")
            }}]`
        )
    }

    authenticate(token: string) {
        return this.send("authenticate",`["${token}"]`)
    }

    invalidate() {
        return this.send("invalidate")
    }

    // TODO: create live query

    select(table: string) {
        return this.send("select",`["${table}"]`)
    }

    create(table: string, values: any | any[]) {
        return this.send("create",`["${table}",${JSON.stringify(values)}]`)
    }

    update(id: string, values: any | any[]) {
        return this.send("update",`["${id}",${JSON.stringify(values)}]`)
    }

    merge(id: string, values: any | any[]) {
        return this.send("merge",`["${id}",${JSON.stringify(values)}]`)
    }

    patch(id: string, values: any | any[]) {
        return this.send("patch",`["${id}",${JSON.stringify(values)}]`)
    }

    delete(id: string) {
        return this.send("delete",`["${id}"]`)
    }

    async query<T extends any[] = Record<string,any>[]>(ql: string, params?: object): Promise<T> {
        const sql = JSON.stringify(ql) + (params ? "," + JSON.stringify(params) : "")

        const response = await this.send("query",`[${sql}]`)

        const errs = [] as string[]
        const results = response.result.map((e:any) => {
            if (e.status === "ERR") {
                errs.push(e.result)
                return
            }
            return e.result
        })

        if (errs.length) {
            throw new SurrealError(errs.join(";"),{ sql })
        }

        return results
    }
}

export default Surreal
