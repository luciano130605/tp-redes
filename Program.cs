// ============================================================
//  INDICE: buscar con Ctrl+F la etiqueta de cada pregunta del PDF
//  [P1.1] hilos vs asincronia      [P1.2] log sin choques
//  [P1.3] [P1.4] lock              [P1.5] fugas con ThreadPool
//  [P2.1] hasta donde leer         [P2.2] como se si hay body
//  [P2.3] path traversal           [P2.4] archivo no soportado
//  [P2.5] CancellationToken        [P2.6] donde se reciben solicitudes
//  [P3.1] Accept-Encoding          [P3.2] que se comprime
//  [P3.3] como se que comprimio    [P4.1] [P4.2] arquitectura
//  [P4.3] using vs try-catch       [D1] SemaphoreSlim
//  [D2] shutdown y Task.Delay      [D3] donde se loguea un POST
// ============================================================

using System.Collections.Concurrent; // BlockingCollection (cola segura entre hilos)
using System.IO.Compression;         // GZipStream / DeflateStream
using System.Net;                    // IPAddress, IPEndPoint
using System.Net.Sockets;            // TcpListener, TcpClient, NetworkStream (sockets)
using System.Text;                   // Encoding, StringBuilder
using System.Text.Json;              // leer config.json
using System.Web;                    // HttpUtility.ParseQueryString

// ============================================================
//  Punto de entrada
// ============================================================
// Lo primero que corre. Leo la config: si no hay config.json uso valores por defecto.
var configPath = args.Length > 0 ? args[0] : "config.json";
var cfg = File.Exists(configPath)
    ? JsonSerializer.Deserialize<ServerConfig>(
          File.ReadAllText(configPath),
          new JsonSerializerOptions { PropertyNameCaseInsensitive = true }) ?? new ServerConfig()
    : new ServerConfig();

// "using var": al terminar el programa llama solo a Dispose() del logger.
using var logger = new DailyLogger(cfg.LogDirectory);

// [P2.5] CancellationToken: es una "señal de apagado".
// El cts la dispara (Cancel) y el token la reciben los metodos para enterarse.
// Lo uso para cerrar el servidor con Ctrl+C: sin esto el bucle de accept
// quedaria bloqueado esperando clientes y habria que matar el proceso a la fuerza.
using var cts = new CancellationTokenSource();

// Ctrl+C: e.Cancel = true evita que se mate el proceso de golpe,
// y cts.Cancel() avisa al servidor que debe cerrar ordenadamente.
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

// [D2] Shutdown: en mi codigo NO hay Task.Delay(300). Si preguntan por el:
// sirve para darle un momento a las solicitudes "en vuelo" a que terminen
// antes de cerrar. Es simple pero "a ojo"; lo correcto seria esperarlas con
// Task.WhenAll. Lo que si hago: el Dispose del logger espera a que se
// escriban todos los logs pendientes antes de salir.
await new WebServer(cfg, logger).RunAsync(cts.Token);
Console.WriteLine("Servidor detenido.");

// ============================================================
//  Configuración externa (requisitos 3 y 4)
// ============================================================
// [P4.1][P4.2] Arquitectura: 3 clases, cada una con una sola responsabilidad:
//   ServerConfig = solo datos de configuracion
//   DailyLogger  = solo escribir logs
//   WebServer    = solo red y HTTP
// Asi, si cambio como se loguea, el servidor ni se entera.
public class ServerConfig
{
    public int Port { get; set; } = 8080;              // puerto donde escucha
    public string WwwRoot { get; set; } = "wwwroot";   // carpeta de archivos a servir
    public string LogDirectory { get; set; } = "logs"; // carpeta de logs
}

// ============================================================
//  Logger: patrón Productor-Consumidor (requisito 9)
//  Muchos hilos producen líneas de log; UN solo hilo consumidor
//  escribe en el archivo del día. Así no hay race condition
//  sobre el archivo y las solicitudes no esperan al disco.
// ============================================================
// [P1.2] Problema: si muchos hilos escriben el mismo archivo a la vez, se pisan
// (race condition). Mi solucion: los hilos solo ENCOLAN la linea (rapido) y
// UN unico hilo consumidor las escribe de a una. Como escribe uno solo, no chocan.
//
// [P1.3][P1.4] Alternativa con lock (por si preguntan):
//   private static readonly object _candado = new object();
//   lock (_candado) { File.AppendAllText(...); }
//   - El lock RECIBE un objeto (el "candado"): todos los hilos usan el MISMO.
//   - Debe ser un tipo por REFERENCIA (no int/bool: se copian y cada hilo
//     bloquearia uno distinto).
//   - Conviene "readonly" para que nadie lo reasigne a otro objeto.
//   - Conviene privado y dedicado (no lock(this) ni lock sobre un string).
//   Elegi la cola porque con lock los hilos de las solicitudes esperarian al disco.
public sealed class DailyLogger : IDisposable
{
    // Cola thread-safe: muchos hilos pueden agregar a la vez sin lock.
    // Guardo la hora junto con la linea para elegir bien el archivo del dia.
    private readonly BlockingCollection<(DateTime Time, string Line)> _queue = new();
    private readonly Task _consumer; // el unico hilo que escribe
    private readonly string _dir;

    public DailyLogger(string dir)
    {
        _dir = dir;
        Directory.CreateDirectory(_dir);
        // LongRunning: hilo propio, porque vive todo el programa (no ocupa el ThreadPool).
        _consumer = Task.Factory.StartNew(Consume, TaskCreationOptions.LongRunning);
    }

    // PRODUCTORES: los hilos de las solicitudes llaman a esto. Solo encola, es instantaneo.
    public void Log(string line) => _queue.Add((DateTime.Now, line));

    // CONSUMIDOR: saca lineas de la cola de a una; si esta vacia, duerme sin gastar CPU.
    private void Consume()
    {
        foreach (var (time, line) in _queue.GetConsumingEnumerable())
        {
            try
            {
                var file = Path.Combine(_dir, $"{time:yyyy-MM-dd}.log"); // un archivo por día
                File.AppendAllText(file, $"{time:HH:mm:ss.fff} {line}{Environment.NewLine}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error escribiendo log: {ex.Message}");
            }
        }
    }

    public void Dispose()
    {
        _queue.CompleteAdding(); // no entran mas lineas; el consumidor vacia la cola
        _consumer.Wait();        // espero que termine: no se pierden logs al cerrar
    }
}

// ============================================================
//  Servidor
// ============================================================
public sealed class WebServer
{
    private const int MaxBodyBytes = 1_000_000;           // limite de 1 MB: evita que un cliente me haga reservar memoria infinita
    private static readonly TimeSpan RequestTimeout = TimeSpan.FromSeconds(15); // tiempo maximo por solicitud

    // [P2.4] Archivo no soportado: este diccionario es una ALLOWLIST (lista de permitidos).
    // Traduce extension -> Content-Type, que es lo que el navegador usa para saber
    // que hacer con el archivo (un .css como text/plain no se aplicaria).
    // Si la extension no esta aca (.exe, .cs...), respondo 404 como si no existiera.
    private static readonly Dictionary<string, string> MimeTypes = new(StringComparer.OrdinalIgnoreCase)
    {
        [".html"] = "text/html; charset=utf-8",
        [".htm"]  = "text/html; charset=utf-8",
        [".css"]  = "text/css; charset=utf-8",
        [".js"]   = "application/javascript; charset=utf-8",
        [".json"] = "application/json; charset=utf-8",
        [".txt"]  = "text/plain; charset=utf-8",
        [".png"]  = "image/png",
        [".jpg"]  = "image/jpeg",
        [".jpeg"] = "image/jpeg",
        [".gif"]  = "image/gif",
        [".svg"]  = "image/svg+xml",
        [".ico"]  = "image/x-icon",
    };

    private readonly ServerConfig _cfg;
    private readonly DailyLogger _log;
    private readonly string _root; // ruta absoluta de wwwroot

    // Contadores compartidos entre hilos. "x++" no es atomico (leer, sumar, escribir)
    // y se podrian perder cuentas, por eso uso Interlocked.
    private int _activeConnections;   // se modifica con Interlocked (sin lock)
    private long _totalRequests;

    public WebServer(ServerConfig cfg, DailyLogger log)
    {
        _cfg = cfg;
        _log = log;
        // Ruta absoluta de la raiz: la uso despues para chequear path traversal.
        _root = Path.TrimEndingDirectorySeparator(Path.GetFullPath(cfg.WwwRoot));
    }

    // ---- Bucle de aceptación: socket -> bind -> listen -> accept ----
    public async Task RunAsync(CancellationToken ct)
    {
        // TcpListener encapsula la API de sockets: el constructor hace socket+bind,
        // Start() hace listen, y AcceptTcpClientAsync hace accept.
        var listener = new TcpListener(IPAddress.Any, _cfg.Port); // socket + bind
        listener.Start();                                         // listen
        Console.WriteLine($"Servidor escuchando en http://localhost:{_cfg.Port}/");
        Console.WriteLine($"Carpeta raíz: {_root}");

        try
        {
            while (!ct.IsCancellationRequested)
            {
                // [P2.6][D3] ACA el servidor RECIBE las conexiones nuevas.
                // Con await el hilo no se bloquea mientras espera. Si se cancela el
                // token (Ctrl+C), esta linea lanza OperationCanceledException y el bucle termina.
                // Aca solo acepto la conexion; el HTTP se lee en HandleClientAsync.
                var client = await listener.AcceptTcpClientAsync(ct); // accept (asíncrono, no bloquea hilos)

                // [P1.1] Hilos vs asincronia: uso las dos y cada una resuelve algo distinto.
                //  - Task.Run (ThreadPool) = CONCURRENCIA: un cliente lento no frena a los demas
                //    y el bucle vuelve enseguida a aceptar. No creo un Thread por cliente
                //    porque es caro en memoria; el ThreadPool reutiliza hilos.
                //  - async/await = EFICIENCIA: leer red/disco es esperar (I/O-bound), y con
                //    await el hilo se libera mientras espera. Mil conexiones no son mil hilos.
                // El "_ =" es "lanzo la tarea y no la espero", asi sigo aceptando.
                _ = Task.Run(() => HandleClientAsync(client, ct));    // una Task por conexión -> ThreadPool (TPL)

                // [D1] SemaphoreSlim: mi servidor NO limita conexiones (el enunciado pide indefinidas).
                // Para limitar a 50 usaria un SemaphoreSlim(50): hago "await WaitAsync()" antes de
                // lanzar la Task y "Release()" en un finally al terminar. La conexion 51 espera
                // sin bloquear hilos hasta que alguien libere un permiso.
            }
        }
        catch (OperationCanceledException) { } // salida normal al cancelar
        finally
        {
            listener.Stop(); // cierra el socket de escucha y libera el puerto
        }
    }

    // ---- Atiende UNA conexión (una solicitud HTTP/1.1, Connection: close) ----
    private async Task HandleClientAsync(TcpClient client, CancellationToken serverCt)
    {
        Interlocked.Increment(ref _activeConnections);
        Interlocked.Increment(ref _totalRequests);

        // La IP sale del socket del cliente (requisito 9).
        var ip = (client.Client.RemoteEndPoint as IPEndPoint)?.Address.ToString() ?? "?";
        // Declaradas afuera del try para que el finally pueda loguearlas aunque algo falle.
        string method = "-", target = "-", query = "", body = "";
        int status = 0;
        long sent = 0;

        // [P1.5] Fugas de memoria con ThreadPool: el pool en si no pierde memoria; las fugas
        // vienen de recursos sin cerrar. Lo controlo con:
        //   1) using (client / stream / reader / cts): cierran todo al salir, aunque haya error.
        //   2) Timeout de 15 s: un cliente que no manda nada no deja una Task colgada para siempre.
        //   3) finally: siempre decrementa el contador y loguea.
        //   4) Tope de 1 MB para el body.
        //
        // [P4.3] using vs try-catch: NO se reemplazan. El using NO captura errores, solo
        // GARANTIZA que se llame a Dispose() (liberar el recurso). El try-catch MANEJA el error.
        // Los uso juntos.
        try
        {
            using (client)
            {
                // Token que se cancela a los 15 s o si se apaga el servidor.
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(serverCt);
                cts.CancelAfter(RequestTimeout); // evita que un cliente lento/colgado retenga recursos
                var ct = cts.Token;

                using var stream = client.GetStream(); // NetworkStream: por aca entran y salen los bytes
                // Latin1: 1 char = 1 byte, así Content-Length (bytes) coincide con los chars leídos
                using var reader = new StreamReader(stream, Encoding.Latin1, false, 8192, leaveOpen: true);

                // [P2.1] Hasta donde leer: una solicitud HTTP tiene esta forma:
                //     GET /ruta HTTP/1.1      <- linea de solicitud (leo 1 linea)
                //     Header: valor           <- headers (leo lineas hasta una VACIA)
                //                             <- linea en blanco = terminaron los headers
                //     cuerpo                  <- solo en POST: leo tantos bytes como diga Content-Length
                // Alternativa a bajo nivel: stream.Read(buffer, 0, buffer.Length), donde 0 es el
                // offset (desde donde guarda en el buffer) y buffer.Length el maximo a leer.
                // Yo uso StreamReader, que hace eso internamente con un buffer de 8192.

                // 1) Línea de solicitud: "GET /ruta?x=1 HTTP/1.1"
                var requestLine = await reader.ReadLineAsync(ct);
                if (string.IsNullOrWhiteSpace(requestLine)) return;

                var parts = requestLine.Split(' '); // [0]=metodo, [1]=ruta, [2]=version
                if (parts.Length < 3)
                {
                    status = 400; // solicitud mal formada
                    sent = await SendAsync(stream, 400, "text/html; charset=utf-8",
                        Encoding.UTF8.GetBytes("<h1>400 Bad Request</h1>"), "", ct);
                    return;
                }
                method = parts[0].ToUpperInvariant();
                target = parts[1];

                // 2) Encabezados: "Nombre: valor" hasta la linea en blanco.
                var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                string? line;
                while (!string.IsNullOrEmpty(line = await reader.ReadLineAsync(ct)))
                {
                    var i = line.IndexOf(':');
                    if (i > 0) headers[line[..i].Trim()] = line[(i + 1)..].Trim();
                }

                // 3) Parámetros de consulta (requisito 7): lo que va despues del "?". Solo se loguean.
                var qIndex = target.IndexOf('?');
                var rawPath = qIndex >= 0 ? target[..qIndex] : target;
                if (qIndex >= 0)
                {
                    var nv = HttpUtility.ParseQueryString(target[(qIndex + 1)..]);
                    query = string.Join("&", nv.AllKeys.Select(k => $"{k}={nv[k]}"));
                }

                // [P2.2] Como se si hay body: miro el header Content-Length. Si existe y es
                // mayor a 0, hay body y ese numero dice cuantos bytes leer. Si no esta
                // (un GET comun), no leo nada: si intentara leer, me quedaria esperando datos que no llegan.
                // 4) Cuerpo del POST (requisito 6): solo se loguea
                if (method == "POST"
                    && headers.TryGetValue("Content-Length", out var cl)
                    && int.TryParse(cl, out var len) && len > 0)
                {
                    len = Math.Min(len, MaxBodyBytes); // nunca mas de 1 MB
                    var buf = new char[len];
                    var read = await reader.ReadBlockAsync(buf.AsMemory(), ct);
                    // Vuelvo a bytes y decodifico como UTF-8 para que las tildes se vean bien en el log.
                    body = Encoding.UTF8.GetString(Encoding.Latin1.GetBytes(buf, 0, read));
                }

                // 5) Armar y enviar respuesta
                var (st, contentType, content) = await BuildResponseAsync(method, rawPath, ct);
                status = st;
                headers.TryGetValue("Accept-Encoding", out var acceptEnc);
                sent = await SendAsync(stream, st, contentType, content, acceptEnc ?? "", ct);
            }
        }
        catch (OperationCanceledException) { status = status == 0 ? 408 : status; } // timeout -> 408
        catch (IOException) { /* el cliente cerró la conexión */ }
        catch (Exception ex)
        {
            Console.WriteLine($"Error atendiendo {ip}: {ex.Message}");
        }
        finally
        {
            // [D3] ACA se registra cada solicitud, incluido el POST (el body va en body=[...]).
            // El finally corre siempre (exito, error o timeout), asi que nada queda sin loguear.
            // Solo encola la linea en el logger [P1.2]; el consumidor la escribe.
            // Requisito 9: IP de origen + datos de la solicitud
            _log.Log($"{ip} {method} {target} status={status} bytes={sent} " +
                     $"query=[{Clean(query)}] body=[{Clean(body)}]");
            Interlocked.Decrement(ref _activeConnections);
        }
    }

    // ---- Decide qué responder (default index.html, 404, 405) ----
    private async Task<(int Status, string ContentType, byte[] Content)> BuildResponseAsync(
        string method, string rawPath, CancellationToken ct)
    {
        // Solo GET y POST; otro metodo -> 405.
        if (method is not ("GET" or "POST"))
            return (405, "text/html; charset=utf-8", Encoding.UTF8.GetBytes("<h1>Metodo no permitido, solo GET o POST</h1>"));

        try
        {
            var relative = Uri.UnescapeDataString(rawPath).TrimStart('/');

            // Requisito 2: si no se especifica archivo, se sirve index.html
            if (relative.Length == 0 || relative.EndsWith('/'))
                relative += "index.html";

            // [P2.3] Path traversal: el ataque es pedir "/../../secreto" para salir de wwwroot.
            // Defensa en 2 pasos:
            //   1) GetFullPath resuelve los "../" y da la ruta REAL final.
            //   2) Verifico que esa ruta empiece con la carpeta raiz. Si no, se salio -> no sirvo nada.
            // Devuelvo 404 (no 403) para no confirmar que el archivo existe.
            var full = Path.GetFullPath(Path.Combine(_root, relative));

            // Seguridad: evitar path traversal ("../../etc/passwd")
            var insideRoot = full.StartsWith(_root + Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase);

            // Sirvo solo si esta dentro de la raiz, existe, y la extension esta en la allowlist [P2.4].
            if (insideRoot && File.Exists(full) && MimeTypes.TryGetValue(Path.GetExtension(full), out var mime))
                return (200, mime, await File.ReadAllBytesAsync(full, ct));
        }
        catch (OperationCanceledException) { throw; }
        catch { /* ruta inválida -> 404 */ }

        return await NotFoundAsync(ct);
    }

    // Requisito 5: 404 con documento personalizado
    private async Task<(int, string, byte[])> NotFoundAsync(CancellationToken ct)
    {
        var custom = Path.Combine(_root, "404.html"); // si no existe, uso un HTML minimo
        var html = File.Exists(custom)
            ? await File.ReadAllBytesAsync(custom, ct)
            : Encoding.UTF8.GetBytes("<h1>404 - Archivo no encontrado</h1>");
        return (404, "text/html; charset=utf-8", html);
    }

    // ---- Escribe la respuesta HTTP con compresión (requisito 8) ----
    private static async Task<long> SendAsync(NetworkStream stream, int status, string contentType,
        byte[] content, string acceptEncoding, CancellationToken ct)
    {
        // [P3.1] Como sabe si acepta GZIP: el navegador manda el header de la solicitud
        // "Accept-Encoding: gzip, deflate" (= "se descomprimir esto"). Yo lo leo y busco
        // gzip; si no, deflate. Si no manda el header, respondo sin comprimir.
        string? encoding = null;
        if (acceptEncoding.Contains("gzip", StringComparison.OrdinalIgnoreCase)) encoding = "gzip";
        else if (acceptEncoding.Contains("deflate", StringComparison.OrdinalIgnoreCase)) encoding = "deflate";

        // [P3.2] Que se comprime: en MI codigo se comprime TODO lo que tenga contenido si el
        // cliente acepta gzip/deflate (no filtro por tipo ni tamaño; esta como limitacion).
        // Lo ideal (criterio del PDF): comprimir solo texto (html, css, js, json, txt, svg) y
        // archivos de mas de ~1 KB. NO comprimir PNG, JPG o ZIP porque ya vienen comprimidos:
        // gasto CPU y casi no ahorro nada. Se haria con una lista de extensiones y la
        // condicion content.Length > 1024 en el if de abajo.
        var payload = content;
        if (encoding != null && content.Length > 0)
        {
            using var ms = new MemoryStream(); // "archivo en memoria" donde queda lo comprimido
            // Patron Decorador: el compresor envuelve al MemoryStream; lo que escribo sale comprimido.
            using (Stream comp = encoding == "gzip"
                       ? new GZipStream(ms, CompressionLevel.Fastest, leaveOpen: true)
                       : new DeflateStream(ms, CompressionLevel.Fastest, leaveOpen: true))
            {
                await comp.WriteAsync(content, ct);
            } // al cerrar el compresor se escribe el final del stream comprimido
            payload = ms.ToArray();
        }

        // Armo la respuesta a mano: linea de estado + headers + linea en blanco + body.
        var sb = new StringBuilder();
        sb.Append($"HTTP/1.1 {status} {Reason(status)}\r\n");
        sb.Append($"Content-Type: {contentType}\r\n");
        sb.Append($"Content-Length: {payload.Length}\r\n"); // sobre los bytes ya comprimidos
        if (encoding != null)
        {
            // [P3.3] Como se que comprimio: por el header de la RESPUESTA "Content-Encoding: gzip",
            // que le avisa al navegador que descomprima. Se verifica con
            // curl -i --compressed http://localhost:8080/  o en F12 -> Network -> Headers.
            sb.Append($"Content-Encoding: {encoding}\r\n");
            sb.Append("Vary: Accept-Encoding\r\n"); // avisa a cachés que la respuesta depende de ese header
        }
        sb.Append("Connection: close\r\n\r\n"); // cierro tras responder; la linea en blanco separa headers y body

        var headerBytes = Encoding.ASCII.GetBytes(sb.ToString());
        await stream.WriteAsync(headerBytes, ct);
        await stream.WriteAsync(payload, ct);
        await stream.FlushAsync(ct);
        return headerBytes.Length + payload.Length;
    }

    private static string Reason(int status) => status switch
    {
        200 => "OK",
        400 => "Bad request",
        404 => "No encontrado",
        405 => "Metodo no permitido",
        408 => "Timeout de solicitud",
        _ => "Error"
    };

    // Evita que un cliente inyecte saltos de línea y falsifique entradas del log
    private static string Clean(string s) =>
        s.Replace("\r", "\\r").Replace("\n", "\\n");
}