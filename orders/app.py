"""Orders — оформление заказа с одной оплатой.

Покупатель получает ответ на POST /orders не позже чем через секунду.
Если исход платежа ещё не известен, ответ pending, а заказ дожимается в фоне.

Деньги списываются не больше одного раза: каждый заказ ходит в Payment
с одним и тем же Idempotency-Key (это id заказа) и одним и тем же телом.
Повтор такого запроса возвращает прежний результат, а не новое списание.
Если ответ потерялся или банк ответил слишком поздно, списание видно через
GET /payments?order_id= — тогда заказ становится paid без повторного списания.

Пока Payment молчит и на запрос, и на поиск списания, новые запросы к нему
не шлём: один пробный POST с паузой 0.5, 1, 2, 4… до 8 с. Когда пробный
запрос получает ответ, очередь pending разбирается дальше.

Отказ банка (402) — единственная причина отклонить заказ: резерв снимается,
статус rejected. 500 и обрыв — не отказ, попытка повторяется.
"""

import json
import logging
import os
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import psycopg

PORT = int(os.environ.get("PORT", "8080"))
DATABASE_URL = os.environ["DATABASE_URL"]
PAYMENT_URL = os.environ["PAYMENT_URL"].rstrip("/")
INVENTORY_URL = os.environ["INVENTORY_URL"].rstrip("/")
# Шина событий; пусто — события не публикуются.
EVENTS_URL = os.environ.get("EVENTS_URL", "").rstrip("/")

# Короткий вызов не даёт POST /orders ждать банк. В фоне ту же попытку можно
# подержать дольше, если банк жив, но отвечает медленно.
SHORT_TIMEOUT = 0.3
GET_TIMEOUT = 0.3
RESERVE_HANDLER = 0.4
RESERVE_WORKER = 2.0
SLOW_CAP = 8.0

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("orders")

SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    id           TEXT PRIMARY KEY,
    user_id      TEXT NOT NULL,
    status       TEXT NOT NULL,
    amount_cents BIGINT NOT NULL,
    items        JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS bonuses (
    user_id TEXT PRIMARY KEY,
    points  BIGINT NOT NULL
)
"""


def db():
    return psycopg.connect(DATABASE_URL, autocommit=True)


def init_db():
    # База может подниматься дольше сервиса — ждём её.
    for attempt in range(60):
        try:
            with db() as conn:
                conn.execute(SCHEMA)
            return
        except psycopg.OperationalError as e:
            log.info("база недоступна (%s), жду", e)
            time.sleep(1)
    raise SystemExit("не дождался базы")


class TransportError(Exception):
    """Сеть, таймаут или обрыв: ответа HTTP нет, исход операции неизвестен."""


def call(method, url, body=None, timeout=2.0, headers=None):
    """HTTP-вызов. Возвращает (код, тело). Сетевой сбой — TransportError."""
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    if body is not None:
        req.add_header("Content-Type", "application/json")
    if headers:
        for key, value in headers.items():
            req.add_header(key, value)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            return resp.status, json.loads(raw or b"null")
    except urllib.error.HTTPError as e:
        try:
            raw = e.read()
        except Exception as read_err:
            raise TransportError(str(read_err)) from read_err
        try:
            parsed = json.loads(raw) if raw else None
        except ValueError:
            parsed = None
        return e.code, parsed
    except Exception as e:
        # Таймаут, обрыв, недочитанное тело: HTTP-ответа нет.
        raise TransportError(str(e)) from e


class PayState:
    """Один вызов Payment за раз и пауза, пока он недоступен.

    lookup_first: последний POST оборвался, сначала ищем списание, а не шлём ещё один.
    down: не ответили ни POST, ни GET. До down_until к банку ходит только пробный POST.
    """

    def __init__(self):
        self.mu = threading.Lock()
        self.busy = False
        self.inflight = set()
        self.down = False
        self.down_until = 0.0
        self.streak = 0
        self.lookup_first = False
        self.needs_lookup = set()
        self.use_slow = set()
        self.ladder = {}
        self.next_try = {}
        self.learned = SHORT_TIMEOUT

    def begin(self, order_id):
        with self.mu:
            if order_id in self.inflight:
                return False
            self.inflight.add(order_id)
            return True

    def end(self, order_id):
        with self.mu:
            self.inflight.discard(order_id)

    def blocked_for(self):
        with self.mu:
            now = time.monotonic()
            if now < self.down_until:
                return self.down_until - now
            if self.busy:
                return 0.05
            return 0.0

    def repair_lookup(self):
        with self.mu:
            if self.lookup_first and not self.needs_lookup:
                self.lookup_first = False

    def schedule(self, order_id, delay):
        with self.mu:
            self.next_try[order_id] = time.monotonic() + delay

    def pick(self, orders):
        with self.mu:
            now = time.monotonic()
            if self.busy or now < self.down_until:
                return None
            lookup_only = self.lookup_first and not self.down
            best = None
            best_key = None
            for order in orders:
                oid = order["id"]
                if oid in self.inflight:
                    continue
                if self.down:
                    key = (order["created"], oid)
                elif lookup_only:
                    if oid not in self.needs_lookup:
                        continue
                    key = (order["created"], oid)
                else:
                    if self.next_try.get(oid, 0) > now and oid not in self.needs_lookup:
                        continue
                    key = (
                        0 if oid in self.needs_lookup else 1,
                        1 if oid in self.use_slow else 0,
                        order["created"],
                        oid,
                    )
                if best is None or key < best_key:
                    best = order
                    best_key = key
            return best

    def prepare(self, order_id, handler):
        """Занять слот Payment. 'post', 'lookup', 'probe' или None."""
        with self.mu:
            if self.busy or time.monotonic() < self.down_until:
                return None
            if handler and (self.lookup_first or self.down):
                return None
            if not handler and self.down:
                mode = "probe"
            elif not handler and (self.lookup_first or order_id in self.needs_lookup):
                if order_id not in self.needs_lookup:
                    return None
                mode = "lookup"
            else:
                mode = "post"
            self.busy = True
            return mode

    def release_busy(self):
        with self.mu:
            self.busy = False

    def post_timeout(self, order_id, handler, probe):
        with self.mu:
            if handler or probe or order_id not in self.use_slow:
                return SHORT_TIMEOUT
            step = self.ladder.get(order_id, 0.6)
            self.ladder[order_id] = min(SLOW_CAP, step * 2)
            return min(SLOW_CAP, max(step, self.learned))

    def on_post_timeout(self, order_id, probe):
        with self.mu:
            if probe:
                self._trip_unlocked()
                return
            self.needs_lookup.add(order_id)
            self.lookup_first = True
            self.next_try.pop(order_id, None)

    def on_get_timeout(self):
        with self.mu:
            self._trip_unlocked()

    def _trip_unlocked(self):
        self.lookup_first = False
        self.streak += 1
        delay = min(8.0, 0.5 * (2 ** (self.streak - 1)))
        self.down_until = time.monotonic() + delay
        self.down = True
        log.warning("payment недоступен, пауза %.1f с", delay)

    def _mark_up_unlocked(self, elapsed):
        was_down = self.down
        self.streak = 0
        self.down_until = 0.0
        self.down = False
        self.lookup_first = False
        if elapsed >= 0.25:
            self.learned = min(SLOW_CAP, max(self.learned, elapsed + 0.4))
        if was_down:
            log.info("payment снова отвечает")

    def _clear_unlocked(self, order_id):
        self.needs_lookup.discard(order_id)
        self.use_slow.discard(order_id)
        self.ladder.pop(order_id, None)
        self.next_try.pop(order_id, None)

    def classify_post(self, order_id, code, elapsed):
        with self.mu:
            self._mark_up_unlocked(elapsed)
            self.needs_lookup.discard(order_id)
            if code in (200, 201):
                self._clear_unlocked(order_id)
                return "paid"
            if code == 402:
                self._clear_unlocked(order_id)
                return "declined"
            if code == 500:
                if elapsed >= 0.25:
                    self.use_slow.add(order_id)
                else:
                    self.use_slow.discard(order_id)
                self.next_try[order_id] = time.monotonic() + 0.02
                return None
            log.warning("order %s: payment %s", order_id, code)
            self.next_try[order_id] = time.monotonic() + 0.5
            return None

    def classify_get(self, order_id, code, body):
        with self.mu:
            self._mark_up_unlocked(0.0)
            if code == 200 and isinstance(body, list) and body:
                self._clear_unlocked(order_id)
                return "paid"
            if code == 200:
                # Ответ быстрый и пустой: списания нет. Банк мог отклонить оплату
                # или вернуть 500, но сам ответ ещё не дошёл — ждём его дольше.
                self.needs_lookup.discard(order_id)
                self.use_slow.add(order_id)
                self.next_try[order_id] = time.monotonic() + 0.05
                return None
            self.next_try[order_id] = time.monotonic() + 0.2
            if order_id in self.needs_lookup:
                self.lookup_first = True
            return None


state = PayState()


def parse_items(raw):
    if isinstance(raw, memoryview):
        raw = bytes(raw)
    if isinstance(raw, bytes):
        raw = raw.decode()
    if isinstance(raw, str):
        raw = json.loads(raw)
    return raw


def load_pending():
    with db() as conn:
        rows = conn.execute(
            """SELECT id, user_id, amount_cents, items, created_at
               FROM orders WHERE status = 'pending'
               ORDER BY created_at, id"""
        ).fetchall()
    orders = []
    for row in rows:
        try:
            items = parse_items(row[3])
        except (TypeError, ValueError):
            log.exception("order %s: битые позиции", row[0])
            continue
        orders.append({
            "id": row[0],
            "user_id": row[1],
            "amount": row[2],
            "items": items,
            "created": row[4],
        })
    return orders


def read_status(order_id):
    with db() as conn:
        row = conn.execute("SELECT status FROM orders WHERE id = %s", (order_id,)).fetchone()
    if row is None:
        return "pending"
    return row[0]


def set_status(order_id, status):
    with db() as conn:
        row = conn.execute(
            "UPDATE orders SET status = %s WHERE id = %s AND status = 'pending' RETURNING id",
            (status, order_id),
        ).fetchone()
    return row is not None


def reserve(order_id, items, timeout):
    try:
        code, _ = call("POST", f"{INVENTORY_URL}/reservations", {
            "order_id": order_id,
            "items": [{"sku": i["sku"], "qty": i["qty"]} for i in items],
        }, timeout=timeout)
    except TransportError as e:
        log.warning("order %s: резерв без ответа (%s)", order_id, e)
        return False
    if code not in (200, 201):
        log.warning("order %s: резерв %s", order_id, code)
        return False
    return True


def release(order_id):
    try:
        code, _ = call(
            "DELETE",
            f"{INVENTORY_URL}/reservations/{urllib.parse.quote(order_id)}",
            timeout=0.5,
        )
    except TransportError as e:
        log.warning("order %s: не снял резерв (%s)", order_id, e)
        return False
    if code not in (200, 204):
        log.warning("order %s: не снял резерв (%s)", order_id, code)
        return False
    return True


def post_payment(order_id, amount, timeout):
    return call(
        "POST",
        f"{PAYMENT_URL}/payments",
        {"order_id": order_id, "amount_cents": amount, "currency": "RUB"},
        timeout=timeout,
        headers={"Idempotency-Key": order_id},
    )


def get_payment(order_id, timeout):
    url = f"{PAYMENT_URL}/payments?order_id={urllib.parse.quote(order_id)}"
    return call("GET", url, timeout=timeout)


def mark_paid(order_id):
    if set_status(order_id, "paid"):
        log.info("order %s: paid", order_id)
        publish_paid(order_id)


def mark_rejected(order_id):
    if set_status(order_id, "rejected"):
        log.info("order %s: rejected", order_id)


def apply_action(order_id, action):
    if action == "paid":
        mark_paid(order_id)
    elif action == "declined":
        if release(order_id):
            mark_rejected(order_id)
        else:
            state.schedule(order_id, 0.2)


def do_lookup(order_id):
    try:
        code, body = get_payment(order_id, GET_TIMEOUT)
    except TransportError as e:
        log.info("order %s: поиск списания без ответа (%s)", order_id, e)
        state.on_get_timeout()
        return None
    return state.classify_get(order_id, code, body)


def do_post(order_id, amount, mode, handler):
    timeout = state.post_timeout(order_id, handler=handler, probe=(mode == "probe"))
    started = time.monotonic()
    try:
        code, _ = post_payment(order_id, amount, timeout)
    except TransportError as e:
        log.info("order %s: оплата без ответа (%s)", order_id, e)
        state.on_post_timeout(order_id, probe=(mode == "probe"))
        return None
    log.info("order %s: payment %s за %.3f с", order_id, code, time.monotonic() - started)
    return state.classify_post(order_id, code, time.monotonic() - started)


def settle_payment(order_id, amount, handler):
    mode = state.prepare(order_id, handler=handler)
    if mode is None:
        return
    action = None
    try:
        if mode == "lookup":
            action = do_lookup(order_id)
        else:
            action = do_post(order_id, amount, mode, handler)
    finally:
        state.release_busy()
    apply_action(order_id, action)


def worker_step():
    state.repair_lookup()
    orders = load_pending()
    if not orders:
        return False
    order = state.pick(orders)
    if order is None:
        return False
    if not state.begin(order["id"]):
        return False
    try:
        if not reserve(order["id"], order["items"], RESERVE_WORKER):
            state.schedule(order["id"], 0.2)
            return True
        settle_payment(order["id"], order["amount"], handler=False)
        return True
    except Exception:
        log.exception("order %s", order["id"])
        state.schedule(order["id"], 0.5)
        return True
    finally:
        state.end(order["id"])


def worker_loop():
    while True:
        try:
            wait = state.blocked_for()
            if wait > 0:
                time.sleep(min(wait, 0.2))
                continue
            if not worker_step():
                time.sleep(0.05)
        except Exception:
            log.exception("фон")
            time.sleep(0.2)


def valid(body):
    if not isinstance(body, dict):
        return False
    if not isinstance(body.get("user_id"), str) or not body["user_id"]:
        return False
    items = body.get("items")
    if not isinstance(items, list) or not items:
        return False
    for i in items:
        if not isinstance(i, dict):
            return False
        if not isinstance(i.get("sku"), str) or not i["sku"]:
            return False
        for k, minimum in (("qty", 1), ("price_cents", 0)):
            v = i.get(k)
            if not isinstance(v, int) or isinstance(v, bool) or v < minimum:
                return False
    return True


def create_order(body):
    order_id = str(uuid.uuid4())
    amount = sum(i["qty"] * i["price_cents"] for i in body["items"])
    if not state.begin(order_id):
        raise RuntimeError("не удалось занять заказ")
    try:
        with db() as conn:
            conn.execute(
                """INSERT INTO orders (id, user_id, status, amount_cents, items)
                   VALUES (%s, %s, 'pending', %s, %s)""",
                (order_id, body["user_id"], amount, json.dumps(body["items"])),
            )
        try:
            if reserve(order_id, body["items"], RESERVE_HANDLER):
                settle_payment(order_id, amount, handler=True)
        except Exception:
            log.exception("order %s", order_id)
        status = read_status(order_id)
        log.info("order %s: ответ %s", order_id, status)
        return {"id": order_id, "status": status}
    finally:
        state.end(order_id)


def publish_paid(order_id):
    """Сообщает в шину, что заказ оплачен. Не задерживает ответ покупателю."""
    if not EVENTS_URL:
        return
    threading.Thread(target=_publish_paid, args=(order_id,), daemon=True).start()


def _publish_paid(order_id):
    try:
        code, _ = call("POST", f"{EVENTS_URL}/events", {
            "id": str(uuid.uuid4()),
            "type": "order.paid",
            "order_id": order_id,
            "occurred_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }, timeout=0.5)
        if code != 202:
            log.warning("order %s: событие не опубликовано (%s)", order_id, code)
    except Exception as e:
        log.warning("order %s: событие не опубликовано (%s)", order_id, e)


def delivery_confirmed(event):
    """Начисляет бонусы за подтверждённую доставку."""
    with db() as conn:
        conn.execute(
            "INSERT INTO bonuses (user_id, points) VALUES (%s, %s) "
            "ON CONFLICT (user_id) DO UPDATE SET points = bonuses.points + EXCLUDED.points",
            (event["user_id"], event["bonus_points"]),
        )


def get_bonus(user_id):
    with db() as conn:
        row = conn.execute("SELECT points FROM bonuses WHERE user_id = %s", (user_id,)).fetchone()
    return {"points": row[0] if row else 0}


def get_order(order_id):
    with db() as conn:
        row = conn.execute(
            "SELECT id, status, amount_cents FROM orders WHERE id = %s", (order_id,)
        ).fetchone()
    if row is None:
        return None
    return {"id": row[0], "status": row[1], "amount_cents": row[2]}


ORDER_PATH = re.compile(r"^/orders/([^/]+)$")
BONUS_PATH = re.compile(r"^/users/([^/]+)/bonus$")


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def send_json(self, code, obj):
        data = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path == "/healthz":
            return self.send_json(200, {"status": "ok"})
        m = ORDER_PATH.match(self.path)
        if m:
            order = get_order(m.group(1))
            if order is None:
                return self.send_json(404, {"error": "not_found"})
            return self.send_json(200, order)
        m = BONUS_PATH.match(self.path)
        if m:
            return self.send_json(200, get_bonus(urllib.parse.unquote(m.group(1))))
        self.send_json(404, {"error": "not_found"})

    def do_POST(self):
        if self.path not in ("/orders", "/webhooks/delivery"):
            return self.send_json(404, {"error": "not_found"})
        length = int(self.headers.get("Content-Length") or 0)
        try:
            body = json.loads(self.rfile.read(length) or b"null")
        except ValueError:
            body = None
        if self.path == "/webhooks/delivery":
            return self.webhook(body)
        if not valid(body):
            return self.send_json(400, {"error": "bad_request"})
        try:
            self.send_json(201, create_order(body))
        except Exception as e:
            log.exception("POST /orders")
            self.send_json(500, {"error": "internal", "message": str(e)})

    def webhook(self, body):
        if not isinstance(body, dict) or not isinstance(body.get("user_id"), str) \
                or not isinstance(body.get("bonus_points"), int) or body["bonus_points"] <= 0:
            return self.send_json(400, {"error": "bad_request"})
        try:
            delivery_confirmed(body)
        except Exception as e:
            log.exception("POST /webhooks/delivery")
            return self.send_json(500, {"error": "internal", "message": str(e)})
        self.send_json(200, {"status": "ok"})

    def log_message(self, fmt, *args):
        pass


if __name__ == "__main__":
    init_db()
    threading.Thread(target=worker_loop, name="orders-settle", daemon=True).start()
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    server.daemon_threads = True
    log.info("orders: слушаю :%d", PORT)
    server.serve_forever()
