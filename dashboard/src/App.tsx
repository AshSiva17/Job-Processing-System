import { useCallback, useEffect, useMemo, useState } from "react";
import type { Job, JobEvent, JobStatus, Stats } from "./types";

const WS_URL = `${window.location.protocol === "https:" ? "wss:" : "ws:"}//${window.location.host}/ws`;
const STATS_POLL_MS = 3000;

function wsStateLabel(state: WebSocket["readyState"] | "idle"): string {
  switch (state) {
    case WebSocket.CONNECTING:
      return "Connecting…";
    case WebSocket.OPEN:
      return "Live";
    case WebSocket.CLOSING:
      return "Closing…";
    case WebSocket.CLOSED:
      return "Disconnected";
    default:
      return "Idle";
  }
}

function StatusBadge({ status }: { status: JobStatus }) {
  return <span className={`badge badge-${status}`}>{status}</span>;
}

function sortJobs(rows: Job[]): Job[] {
  return [...rows].sort(
    (a, b) =>
      new Date(b.updated_at).getTime() - new Date(a.updated_at).getTime()
  );
}

export function App() {
  const [jobs, setJobs] = useState<Job[]>([]);
  const [stats, setStats] = useState<Stats | null>(null);
  const [wsState, setWsState] = useState<WebSocket["readyState"] | "idle">(
    "idle"
  );
  const [error, setError] = useState<string | null>(null);

  const refreshStats = useCallback(async () => {
    const res = await fetch("/stats");
    if (!res.ok) {
      throw new Error(`Stats request failed (${res.status})`);
    }
    const data = (await res.json()) as Stats;
    setStats(data);
  }, []);

  const loadInitial = useCallback(async () => {
    const [jobsRes, statsRes] = await Promise.all([
      fetch("/jobs?limit=100"),
      fetch("/stats"),
    ]);
    if (!jobsRes.ok) {
      throw new Error(`Jobs request failed (${jobsRes.status})`);
    }
    if (!statsRes.ok) {
      throw new Error(`Stats request failed (${statsRes.status})`);
    }
    const jobsBody = (await jobsRes.json()) as { jobs: Job[] };
    const statsBody = (await statsRes.json()) as Stats;
    setJobs(sortJobs(jobsBody.jobs));
    setStats(statsBody);
  }, []);

  useEffect(() => {
    let cancelled = false;
    void loadInitial().catch((err: unknown) => {
      if (!cancelled) {
        setError(err instanceof Error ? err.message : String(err));
      }
    });
    return () => {
      cancelled = true;
    };
  }, [loadInitial]);

  useEffect(() => {
    const timer = setInterval(() => {
      void refreshStats().catch(() => {
        /* keep last stats on transient failure */
      });
    }, STATS_POLL_MS);
    return () => clearInterval(timer);
  }, [refreshStats]);

  useEffect(() => {
    let socket: WebSocket | null = null;
    let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
    let unmounted = false;

    const connect = () => {
      if (unmounted) {
        return;
      }
      socket = new WebSocket(WS_URL);
      setWsState(socket.readyState);

      socket.addEventListener("open", () => {
        setWsState(WebSocket.OPEN);
        setError(null);
        void refreshStats();
      });

      socket.addEventListener("message", (event) => {
        let payload: JobEvent;
        try {
          payload = JSON.parse(String(event.data)) as JobEvent;
        } catch {
          return;
        }
        setJobs((prev) => {
          const next = new Map(prev.map((job) => [job.id, job]));
          next.set(payload.job.id, payload.job);
          return sortJobs([...next.values()]);
        });
        void refreshStats();
      });

      socket.addEventListener("close", () => {
        setWsState(WebSocket.CLOSED);
        if (!unmounted) {
          reconnectTimer = setTimeout(connect, 2000);
        }
      });

      socket.addEventListener("error", () => {
        setWsState(WebSocket.CLOSED);
      });
    };

    connect();

    return () => {
      unmounted = true;
      if (reconnectTimer) {
        clearTimeout(reconnectTimer);
      }
      socket?.close();
    };
  }, [refreshStats]);

  const connectionAttr = useMemo(() => {
    if (wsState === WebSocket.OPEN) {
      return "open";
    }
    if (error || wsState === WebSocket.CLOSED) {
      return "error";
    }
    return "connecting";
  }, [error, wsState]);

  return (
    <>
      <header>
        <h1>Job dashboard</h1>
        <span className="connection" data-state={connectionAttr}>
          WebSocket: {wsStateLabel(wsState)}
        </span>
      </header>

      {error ? <div className="error-banner">{error}</div> : null}

      <section className="stats" aria-label="Queue summary">
        <div className="stat-card">
          <div className="stat-label">Queue depth</div>
          <div className="stat-value">{stats?.queue_depth ?? "—"}</div>
        </div>
        <div className="stat-card">
          <div className="stat-label">In progress</div>
          <div className="stat-value">{stats?.in_progress ?? "—"}</div>
        </div>
        <div className="stat-card">
          <div className="stat-label">Dead letter</div>
          <div className="stat-value">{stats?.dead_letter ?? "—"}</div>
        </div>
      </section>

      <div className="table-wrap">
        {jobs.length === 0 ? (
          <p className="empty">No jobs yet. Submit one via POST /jobs.</p>
        ) : (
          <table>
            <thead>
              <tr>
                <th>Job</th>
                <th>Type</th>
                <th>Status</th>
                <th>Attempts</th>
                <th>Updated</th>
              </tr>
            </thead>
            <tbody>
              {jobs.map((job) => (
                <tr key={job.id}>
                  <td className="mono" title={job.id}>
                    {job.id.slice(0, 8)}…
                  </td>
                  <td>{job.type}</td>
                  <td>
                    <StatusBadge status={job.status} />
                  </td>
                  <td>
                    {job.attempts}/{job.max_attempts}
                  </td>
                  <td>{new Date(job.updated_at).toLocaleString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </>
  );
}
