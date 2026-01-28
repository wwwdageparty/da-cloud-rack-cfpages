// ================== Cloudflare Pages Function ==================
export async function onRequest(context) {
  const { request, env, waitUntil } = context;
  const url = new URL(request.url);

  if (request.method !== "POST") {
    return new Response("Method Not Allowed", { status: 405 });
  }

  return await handleApi(request, env, waitUntil);
}


// ================== Operator ==================
async function daCreateTable(db, tableName, c1Unique) {
  const c1Constraint = c1Unique ? "UNIQUE" : "";

  const tableSql = `
    CREATE TABLE IF NOT EXISTS ${tableName} (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      c1 VARCHAR(255) ${c1Constraint},
      c2 VARCHAR(255), c3 VARCHAR(255),
      i1 INT, i2 INT, i3 INT,
      d1 DOUBLE, d2 DOUBLE, d3 DOUBLE,
      t1 TEXT, t2 TEXT, t3 TEXT,
      v1 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      v2 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      v3 TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `;

  const statements = [db.prepare(tableSql)];
  
  // Add indices based on uniqueness requirements
  if (!c1Unique) {
    statements.push(db.prepare(`CREATE INDEX IF NOT EXISTS idx_${tableName}_c1 ON ${tableName}(c1)`));
  }
  statements.push(db.prepare(`CREATE INDEX IF NOT EXISTS idx_${tableName}_v2 ON ${tableName}(v2)`));

  return await db.batch(statements);
}

async function daSystemTableInit(db) {
  const tableName = DB_DA_SYSTEM_TABLENAME;
  const newUuid = crypto.randomUUID();

  // Step 1: Create the table using the standard helper
  await daCreateTable(db, tableName, true);

  // Step 2: Insert reserved records (using OR IGNORE to prevent duplicates)
  const versionInsert = `
    INSERT OR IGNORE INTO ${tableName} (id, c1, c2, i1, d1)
    VALUES (1, '___basic_db_version', ?, ?, ?);
  `;
  const systemReserveInsert = `
    INSERT OR IGNORE INTO ${tableName} (id, c1) VALUES (100, '___systemReserve');
  `;

  return await db.batch([
    db.prepare(versionInsert).bind(newUuid, DB_VERSION, DB_VERSION),
    db.prepare(systemReserveInsert)
  ]);
}

// ================== Core API ==================
async function handleApiRequest(action: string, payload: any, db: any, waitUntilFn: any) {
  const tableName = resolveTableName(payload);
  if (!tableName) {
    await errDelegate("Invalid or missing table_name", waitUntilFn);
    return { error: "Invalid or missing table_name" };
  }

  try {
    switch (action) {
      // ---------- INIT ----------
      case "init_system": {
        const results = await daSystemTableInit(db);
        return { success: true, message: "System table and reserved records initialized." };
      }
      // ---------- EXEC ----------
      case "exec": {
        const { query, params } = payload;
        if (!query) return { error: "Missing SQL query" };

        const lowerQuery = query.toLowerCase().trim();
        if (lowerQuery.includes("sqlite_master") || lowerQuery.includes("_cf_")) {
          return { error: "Access to system tables via EXEC is forbidden." };
        }

        try {
          const stmt = db.prepare(query);
          const result = params && Array.isArray(params)
            ? await stmt.bind(...params).all()
            : await stmt.all();
          return { success: true, results: result.results || [], meta: result.meta };
        } catch (err: any) {
          return { error: `SQL Execution Error: ${err.message}` };
        }
      }

      // ---------- CREATE TABLE ----------
     case "create_table": {
        const c1Unique = !!payload.c1_unique;
        await daCreateTable(db, tableName, c1Unique);
        return { message: `Table ${tableName} ready.` };
      }

      // ---------- LIST TABLES ----------
      case "list_tables": {
        const { results } = await db.prepare(`
          SELECT name FROM sqlite_master
          WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '_cf_%'
        `).all();
        return { count: results.length, tables: results.map((r: any) => r.name) };
      }

      // ---------- BATCH INSERT ----------
      case "batch_post": {
        const records = payload.data;
        if (!Array.isArray(records)) return { error: "Payload 'data' must be an array" };

        const statements = records.map((record: any) => {
          normalizeJsonColumns(record);
          const keys = Object.keys(record).filter(k => allowedColumns.includes(k));
          const placeholders = keys.map(() => "?").join(",");
          const sql = `INSERT INTO ${tableName} (${keys.join(",")}) VALUES (${placeholders})`;
          const values = keys.map(k => record[k]);
          return db.prepare(sql).bind(...values);
        });

        const results = await db.batch(statements);
        return { inserted: results.length };
      }

      // ---------- DROP TABLE ----------
      case "drop_table": {
        const sql = `DROP TABLE IF EXISTS ${tableName}`;
        await db.prepare(sql).run();
        return { message: `Table ${tableName} has been deleted.` };
      }

      // ---------- CREATE INDEX ----------
      case "create_index": {
        const col = payload.column;
        if (!col || !allowedColumns.includes(col)) return { error: `Invalid column: ${col}` };
        const indexName = `idx_${tableName}_${col}`;
        const uniqueStr = payload.unique ? "UNIQUE" : "";
        const sql = `CREATE ${uniqueStr} INDEX IF NOT EXISTS ${indexName} ON ${tableName} (${col})`;
        await db.prepare(sql).run();
        return { message: `Index ${indexName} created.` };
      }

      case "list_indices": {
        const { results } = await db.prepare(`PRAGMA index_list(${tableName})`).all();
        return { indices: results };
      }

      case "drop_index": {
        const col = payload.column;
        if (!col) return { error: "Missing column" };
        const indexName = `idx_${tableName}_${col}`;
        await db.prepare(`DROP INDEX IF EXISTS ${indexName}`).run();
        return { message: `Index ${indexName} dropped.` };
      }

      // ---------- INSERT SINGLE ----------
      case "post": {
        normalizeJsonColumns(payload);
        const keys = Object.keys(payload).filter(k => k !== "table_name");
        const invalidKeys = keys.filter(k => !allowedColumns.includes(k));
        if (invalidKeys.length) return { error: `Invalid columns: ${invalidKeys.join(", ")}` };
        const placeholders = keys.map(() => "?").join(",");
        const sql = `INSERT INTO ${tableName} (${keys.join(",")}) VALUES (${placeholders})`;
        const values = keys.map(k => payload[k]);
        await db.prepare(sql).bind(...values).run();
        return null;
      }

      // ---------- UPDATE ----------
      case "put": {
        const hasId = payload.id != null;
        const hasC1 = payload.c1 != null;
        if (!hasId && !hasC1) return { error: "Missing 'id' or 'c1' for update" };
        normalizeJsonColumns(payload);
        const keysPut = Object.keys(payload).filter(k => !["table_name", "id", "c1"].includes(k));
        const invalidKeys = keysPut.filter(k => !allowedColumns.includes(k));
        if (invalidKeys.length) return { error: `Invalid columns: ${invalidKeys.join(", ")}` };

        const whereClause = hasId ? "id = ?" : "c1 = ?";
        const whereValue = hasId ? payload.id : payload.c1;

        let sql, values;
        if (keysPut.length === 0) {
          sql = `UPDATE ${tableName} SET v2 = CURRENT_TIMESTAMP WHERE ${whereClause}`;
          values = [whereValue];
        } else {
          const setClause = keysPut.map(k => `${k} = ?`).join(", ");
          sql = `UPDATE ${tableName} SET ${setClause}, v2 = CURRENT_TIMESTAMP WHERE ${whereClause}`;
          values = [...keysPut.map(k => payload[k]), whereValue];
        }

        const result = await db.prepare(sql).bind(...values).run();
        if (result.changes === 0) return { error: "Record not found or no rows updated" };
        return { updated: result.changes };
      }

      // ---------- UPDATE C1 ----------
      case "update_c1": {
        if (!payload.id || !payload.new_c1) return { error: "Missing id or new_c1" };
        const sql = `UPDATE ${tableName} SET c1 = ?, v2 = CURRENT_TIMESTAMP WHERE id = ?`;
        try {
          const result = await db.prepare(sql).bind(payload.new_c1, payload.id).run();
          if (result.changes === 0) return { error: "Record not found" };
          return { renamed: true };
        } catch (err: any) {
          if (err.message.includes("UNIQUE")) return { error: "c1 already exists" };
          throw err;
        }
      }

      // ---------- QUERY ----------
      case "get": {
        const { table_name, ...options } = payload;
        const columnFilters = Object.keys(options).filter(k => !allowedQueryOptions.includes(k));
        for (const k of columnFilters) if (!allowedColumns.includes(k)) return { error: `Invalid column: ${k}` };

        let query = `SELECT * FROM ${tableName}`;
        const params: any[] = [];
        const conditions: string[] = [];

        const order = options.order === "desc" ? "DESC" : "ASC";
        const orderBy = allowedColumns.includes(options.orderby) ? options.orderby : "id";

        if (options.minId != null && options.offset != null) return { error: "Cannot use both minId and offset" };

        for (const k of columnFilters) {
          conditions.push(`${k} = ?`);
          params.push(options[k]);
        }

        if (options.offset != null) {
          conditions.push(`${orderBy} ${order === "ASC" ? ">" : "<"} ?`);
          params.push(options.offset);
        } else if (options.minId != null) {
          conditions.push(`${orderBy} ${order === "ASC" ? ">" : "<"} ?`);
          params.push(options.minId);
        }

        if (conditions.length) query += " WHERE " + conditions.join(" AND ");
        query += ` ORDER BY ${orderBy} ${order}`;

        const limit = Number.isInteger(options.limit) && options.limit > 0 ? Math.min(options.limit, 500) : 100;
        query += ` LIMIT ${limit}`;

        const result = await db.prepare(query).bind(...params).all();
        return { rows: result.results || [] };
      }

      // ---------- DELETE ----------
      case "delete": {
        const keys = Object.keys(payload).filter(k => k !== "table_name");
        const invalidKeys = keys.filter(k => !allowedColumns.includes(k));
        if (invalidKeys.length) return { error: `Invalid columns: ${invalidKeys.join(", ")}` };

        let sql: string, values: any[] = [];
        if (keys.length === 0) {
          sql = `DELETE FROM ${tableName}`;
          await errDelegate(`DELETE ALL from ${tableName}`, waitUntilFn);
        } else {
          sql = `DELETE FROM ${tableName} WHERE ${keys.map(k => `${k} = ?`).join(" AND ")}`;
          values = keys.map(k => payload[k]);
        }

        const result = await db.prepare(sql).bind(...values).run();
        return { deleted: result.changes ?? 0 };
      }

      default:
        return { error: `Unknown action: ${action}` };
    }
  } catch (err: any) {
    await errDelegate(`DB operation failed: ${err.message}`, waitUntilFn);
    return { error: err.message };
  }
}

// ================== HTTP Wrapper ==================
// async function handleApi(request: Request, env: any, waitUntilFn: any) {
//   const auth = request.headers.get("Authorization");
//   if (!auth || !auth.startsWith("Bearer ")) return nack("unknown", "UNAUTHORIZED", "Missing Authorization");

//   if (auth.split(" ")[1] !== env.DA_WRITE_TOKEN) return nack("unknown", "INVALID_TOKEN", "Token failed");

//   let body: any;
//   try { body = await request.json(); } catch { return nack("unknown", "INVALID_JSON", "Malformed JSON"); }
//   const requestId = body.request_id || "unknown";
//   if (!body.payload) return nack(requestId, "INVALID_FIELD", "Missing payload");

//   const ret = await handleApiRequest(body.action || "", body.payload, env.DB, waitUntilFn);
//   if (ret && ret.error) return nack(requestId, "REQUEST_FAILED", ret.error);

//   return ack(requestId, ret || {});
// }
async function handleApi(request: Request, env: any, waitUntilFn: any) {
  const instanceId = env.DA_INSTANCEID || G_INSTANCE;
  const sourceId = `${C_SERVICE}/${instanceId}`;

  const auth = request.headers.get("Authorization");
  if (!auth || !auth.startsWith("Bearer ")) {
    return nack("unknown", sourceId, "UNAUTHORIZED", "Missing Authorization");
  }

  if (auth.split(" ")[1] !== env.DA_WRITE_TOKEN) {
    return nack("unknown", sourceId, "INVALID_TOKEN", "Token failed");
  }

  let body: any;
  try { 
    body = await request.json(); 
  } catch { 
    return nack("unknown", sourceId, "INVALID_JSON", "Malformed JSON"); 
  }

  const requestId = body.request_id || "unknown";
  if (!body.payload) {
    return nack(requestId, sourceId, "INVALID_FIELD", "Missing payload");
  }

  const ret = await handleApiRequest(body.action || "", body.payload, env.DB, waitUntilFn);
  
  if (ret && ret.error) {
    return nack(requestId, sourceId, "REQUEST_FAILED", ret.error);
  }

  return ack(requestId, sourceId, ret || {});
}

// ================== HELPERS ==================
async function errDelegate(msg: string, waitUntilFn: any) { console.error(msg); waitUntilFn(Promise.resolve()); }
function ack(requestId: string, sourceId: string, payload: any = {}) { 
  return jsonResponse({ type: "ack", request_id: requestId, source_id: sourceId, payload }); 
}

function nack(requestId: string, sourceId: string, code: string, message: string) { 
  return jsonResponse({ 
    type: "nack", 
    request_id: requestId, 
    source_id: sourceId,
    payload: { status: "error", code, message } 
  }, 400); 
}

function jsonResponse(obj: any, status = 200) { 
  return new Response(JSON.stringify(obj, null, 2), { 
    status, 
    headers: { "Content-Type": "application/json" } 
  }); 
}

// ================== TABLE HELPERS ==================
function normalizeJsonColumns(obj: any) { for (const k of ["t1","t2","t3"]) { if (obj[k] !== null && typeof obj[k] === "object") obj[k] = JSON.stringify(obj[k]); } }
const FORBIDDEN_TABLES = new Set(["sqlite_master","sqlite_schema","sqlite_temp_master","sqlite_sequence"]);
function resolveTableName(payload: any) { if (!payload.table_name?.trim()) return null; const name = payload.table_name.trim(); if (FORBIDDEN_TABLES.has(name)) return null; return name; }

// ================== GLOBALS ==================
const allowedColumns = ["id","c1","c2","c3","i1","i2","i3","d1","d2","d3","t1","t2","t3","v1","v2","v3"];
const allowedQueryOptions = ["minId","offset","order","orderby","limit","table_name"];
const DB_DA_SYSTEM_TABLENAME = "__DA_SYSTEM_CONFIG";
const C_SERVICE = "da-cloud-cfd1-rack";
const C_VERSION = "0.0.1";
let G_INSTANCE = "default";
