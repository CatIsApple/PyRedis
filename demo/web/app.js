// 로그인 데모 화면: 프레임워크 없이 DOM API만 쓴다.
// 상태(state)를 바꾸고 render()로 #root를 다시 그린다. 모든 서버 호출은 데모 앱(/api/*)으로만 간다.

const state = {
    status: "loading", // loading | anonymous | authenticated
    username: "",
    loginName: "",
    loginPassword: "",
    busy: false,
    error: "",
    notice: "",
    ttlMs: 0, // 세션 유효 시간 (서버 설정값)
    expiresAt: 0, // 화면에 표시하는 세션 만료 시각 (ms)
    source: "redis", // db | redis
    result: "아직 불러오지 않았습니다.",
    message: "",
    history: [],
    bench: null,
    benchRunning: false,
};

// DOM 요소를 만드는 작은 도우미. 텍스트는 textContent로 넣어서 HTML로 해석되지 않는다.
function el(tag, props = {}, ...children) {
    const node = document.createElement(tag);
    for (const [key, value] of Object.entries(props)) {
        if (key.startsWith("on")) node.addEventListener(key.slice(2).toLowerCase(), value);
        else if (value === true) node.setAttribute(key, "");
        else if (value !== false && value != null) node.setAttribute(key, value);
    }
    for (const child of children.flat()) {
        if (child != null && child !== false) node.append(child instanceof Node ? child : document.createTextNode(String(child)));
    }
    return node;
}

async function api(method, path, body) {
    const res = await fetch(path, {
        method,
        headers: body ? { "Content-Type": "application/json" } : {},
        body: body ? JSON.stringify(body) : undefined,
    });
    const data = await res.json().catch(() => ({}));
    return { status: res.status, ok: res.ok, data, headers: res.headers };
}

const formatRemaining = (ms) => {
    const sec = Math.max(0, Math.ceil(ms / 1000));
    return `${String(Math.floor(sec / 60)).padStart(2, "0")}:${String(sec % 60).padStart(2, "0")}`;
};

const fmt = (ms) => (ms == null ? "-" : `${ms.toFixed(2)}ms`);

function summarize(samples) {
    const sorted = [...samples].sort((a, b) => a - b);
    const pick = (p) => sorted[Math.max(0, Math.ceil((p / 100) * sorted.length) - 1)];
    return { count: sorted.length, mean: sorted.reduce((s, v) => s + v, 0) / sorted.length, p50: pick(50), p95: pick(95) };
}

// ---- 화면 조각 ----

function loginView() {
    const submit = async (event) => {
        event.preventDefault();
        if (state.busy) return;
        Object.assign(state, { busy: true, error: "", notice: "" });
        render();
        try {
            const r = await api("POST", "/api/login", { username: state.loginName, password: state.loginPassword });
            if (r.ok) {
                const me = await api("GET", "/api/me");
                state.loginPassword = "";
                if (me.ok) startSession(me.data);
                else state.error = "로그인 상태를 확인하지 못했습니다.";
            } else if (r.status === 429) state.error = `로그인 시도가 너무 많습니다. ${r.headers.get("Retry-After")}초 뒤에 다시 시도하세요.`;
            else if (r.status === 401) state.error = "아이디 또는 비밀번호가 올바르지 않습니다.";
            else if (r.status === 503) state.error = "세션 저장소를 사용할 수 없습니다. 잠시 뒤 다시 시도하세요.";
            else state.error = "로그인에 실패했습니다.";
        } catch {
            state.error = "서버에 연결할 수 없습니다.";
        }
        state.busy = false;
        render();
    };

    return el(
        "form",
        { class: "card", onSubmit: submit },
        el("h2", {}, "로그인"),
        el("div", { class: "field" }, el("label", { for: "username" }, "아이디"), el("input", { id: "username", type: "text", autocomplete: "username", value: state.loginName, required: true, onInput: (e) => (state.loginName = e.target.value) })),
        el("div", { class: "field" }, el("label", { for: "password" }, "비밀번호"), el("input", { id: "password", type: "password", autocomplete: "current-password", value: state.loginPassword, required: true, onInput: (e) => (state.loginPassword = e.target.value) })),
        el("p", { class: "error", role: "alert" }, state.error),
        el("button", { type: "submit", disabled: state.busy }, state.busy ? "로그인 중…" : "로그인"),
        el("p", { class: "muted" }, "데모 계정: demo / demo1234 (로컬 테스트용 값)")
    );
}

function dashboardView() {
    const sourceButton = (value, label) =>
        el(
            "button",
            { type: "button", role: "radio", "aria-checked": String(state.source === value), class: state.source === value ? "active" : undefined, onClick: () => { state.source = value; render(); } },
            label
        );

    const bench = state.bench
        ? el(
              "div",
              { class: "bench" },
              el(
                  "table",
                  { class: "bench-table" },
                  el("thead", {}, el("tr", {}, ["구분", "평균", "중앙값", "p95", "횟수"].map((t) => el("th", {}, t)))),
                  el(
                      "tbody",
                      {},
                      state.bench.rows.map((r) => el("tr", {}, el("td", {}, r.label), el("td", {}, fmt(r.mean)), el("td", {}, fmt(r.p50)), el("td", {}, fmt(r.p95)), el("td", {}, String(r.count))))
                  )
              ),
              el("p", { class: "muted" }, `브라우저에서 잰 참고용 수치입니다 (요청 왕복 시간, ${state.bench.n}회). 공식 수치는 python -m bench.benchmark --spawn 으로 잽니다.`)
          )
        : null;

    return el(
        "div",
        { class: "stack" },
        el("section", { class: "card" }, el("h2", {}, `${state.username}님, 환영합니다`), el("p", { class: "muted" }, "세션 남은 시간 ", el("span", { class: "countdown" }, formatRemaining(state.expiresAt - Date.now()))), el("div", { class: "row" }, el("button", { type: "button", class: "secondary", onClick: logout }, "로그아웃"))),
        el(
            "section",
            { class: "card" },
            el("h2", {}, "대시보드 (진짜 SQL 조회 + Redis 캐시)"),
            el("span", { class: "label" }, "데이터 출처"),
            el("div", { class: "segmented", role: "radiogroup" }, sourceButton("db", "DBMS 직접 (SQLite)"), sourceButton("redis", "Redis 캐시 + DBMS")),
            el("p", { class: "muted" }, "Redis 캐시 모드에서도 캐시에 값이 없으면 DBMS를 읽습니다. DBMS 직접 모드는 매번 SQL을 실행합니다."),
            el(
                "div",
                { class: "row" },
                el("button", { type: "button", disabled: state.benchRunning, onClick: loadDashboard }, "대시보드 불러오기"),
                el("button", { type: "button", class: "secondary", disabled: state.benchRunning, onClick: clearCache }, "캐시 지우기"),
                el("button", { type: "button", class: "secondary", disabled: state.benchRunning, onClick: runBench }, state.benchRunning ? "측정 중…" : "벤치마크 실행")
            ),
            el("p", { class: "result" }, state.result),
            el("p", { class: "muted", role: "status" }, state.message),
            bench,
            el("h2", {}, "최근 응답 시간"),
            el("ol", { class: "history" }, state.history.map((line) => el("li", {}, line)))
        )
    );
}

// ---- 동작 ----

function startSession(me) {
    Object.assign(state, { status: "authenticated", username: me.username, ttlMs: me.expiresInMs, expiresAt: Date.now() + me.expiresInMs, message: "", bench: null });
    render();
}

function endSession(text) {
    Object.assign(state, { status: "anonymous", notice: text, result: "아직 불러오지 않았습니다.", history: [], message: "", bench: null });
    render();
}

async function logout() {
    await api("POST", "/api/logout", {}).catch(() => {});
    state.loginName = "";
    endSession("로그아웃했습니다.");
}

async function loadDashboard() {
    const r = await api("GET", `/api/dashboard?mode=${state.source}`).catch(() => null);
    if (r?.status === 401) return endSession("세션이 만료되었습니다. 다시 로그인하세요.");
    if (!r?.ok) {
        state.message = "대시보드를 불러오지 못했습니다.";
        return render();
    }
    const cache = r.headers.get("X-Cache");
    const label = cache === "HIT" ? "Redis 캐시 HIT" : cache === "MISS" ? "DBMS 조회 (캐시 MISS, 결과를 Redis에 저장)" : "DBMS 직접 조회";
    state.result = `${label}: ${r.data.elapsedMs}ms. 이벤트 ${r.data.data.totalEvents.toLocaleString()}건, 금액 합계 ${r.data.data.totalAmount.toLocaleString()}`;
    state.history = [...state.history, `${label} ${r.data.elapsedMs}ms`].slice(-6);
    state.message = "";
    state.expiresAt = Date.now() + state.ttlMs; // 서버가 요청마다 세션을 연장하므로 화면의 남은 시간도 다시 맞춘다
    render();
}

async function clearCache() {
    const r = await api("DELETE", "/api/dashboard/cache").catch(() => null);
    if (r?.status === 401) return endSession("세션이 만료되었습니다. 다시 로그인하세요.");
    state.message = r?.ok ? (r.data.deleted ? "캐시를 지웠습니다. 다음 요청은 원본을 다시 조회합니다." : "지울 캐시가 없습니다.") : "캐시를 지우지 못했습니다.";
    render();
}

// 브라우저에서 두 방식을 N번씩 불러 평균·중앙값·p95를 낸다 (참고용)
async function runBench() {
    const N = 30;
    Object.assign(state, { benchRunning: true, bench: null, message: "" });
    render();
    const timed = async (path) => {
        const t0 = performance.now();
        const r = await api("GET", path);
        if (!r.ok) throw Object.assign(new Error("failed"), { status: r.status });
        return { ms: performance.now() - t0, cache: r.headers.get("X-Cache") };
    };
    try {
        await timed("/api/dashboard?mode=db"); // 워밍업 1회 (결과에서 제외)
        const dbSamples = [];
        for (let i = 0; i < N; i++) dbSamples.push((await timed("/api/dashboard?mode=db")).ms);
        await api("DELETE", "/api/dashboard/cache");
        const miss = await timed("/api/dashboard?mode=redis");
        const hitSamples = [];
        for (let i = 0; i < N; i++) {
            const r = await timed("/api/dashboard?mode=redis");
            if (r.cache === "HIT") hitSamples.push(r.ms);
        }
        const row = (label, samples) => ({ label, ...summarize(samples) });
        state.bench = { n: N, rows: [row("DBMS 직접 (SQLite)", dbSamples), row("Redis 캐시 MISS (DBMS + 저장)", [miss.ms]), row("Redis 캐시 HIT", hitSamples)] };
    } catch (err) {
        if (err.status === 401) endSession("세션이 만료되었습니다. 다시 로그인하세요.");
        else state.message = "벤치마크를 끝내지 못했습니다.";
    }
    state.benchRunning = false;
    render();
}

// ---- 루트 ----

function render() {
    const root = document.getElementById("root");
    let screen;
    if (state.status === "loading") screen = el("section", { class: "card" }, el("p", { class: "muted" }, "세션을 확인하는 중…"));
    else if (state.status === "anonymous") screen = loginView();
    else screen = dashboardView();
    root.replaceChildren(el("h1", {}, "미니 Redis 로그인 데모"), el("p", { class: "notice", role: "status" }, state.notice), screen);
}

(async function init() {
    render();
    try {
        const r = await api("GET", "/api/me");
        if (r.ok) startSession(r.data);
        else Object.assign(state, { status: "anonymous" });
    } catch {
        state.error = "서버에 연결할 수 없습니다.";
        state.status = "anonymous";
    }
    render();
    // 로그인 상태일 때만 1초마다 남은 시간을 다시 그린다
    setInterval(() => {
        if (state.status === "authenticated") render();
    }, 1000);
})();
