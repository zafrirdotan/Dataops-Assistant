/**
 * Converts a cron schedule string to human-readable time.
 * Cron format: minute hour dayOfMonth month dayOfWeek (5 fields).
 * Returns the original string for "manual" or unknown patterns.
 */
export function formatScheduleToReadable(schedule: string): string {
  const s = schedule.trim().toLowerCase();
  if (s === "manual" || s === "") return "Manual";

  const parts = s.split(/\s+/);
  if (parts.length < 5) return schedule;

  const [minute, hour, dayOfMonth, month, dayOfWeek] = parts;

  const formatTime = (h: string, m: string): string => {
    const hNum = parseInt(h, 10);
    const mNum = parseInt(m, 10);
    if (Number.isNaN(hNum) || Number.isNaN(mNum)) return `${h}:${m}`;
    const ampm = hNum >= 12 ? "PM" : "AM";
    const h12 = hNum % 12 || 12;
    const mStr = mNum < 10 ? `0${mNum}` : String(mNum);
    return `${h12}:${mStr} ${ampm}`;
  };

  const dayNames = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];

  // Every N minutes
  if (minute.startsWith("*/") && hour === "*" && dayOfMonth === "*" && month === "*" && dayOfWeek === "*") {
    const n = minute.slice(2);
    const num = parseInt(n, 10);
    if (!Number.isNaN(num)) return `Every ${num} minute${num === 1 ? "" : "s"}`;
  }

  // Hourly at minute 0
  if (minute !== "*" && !minute.includes("/") && hour === "*" && dayOfMonth === "*" && month === "*" && dayOfWeek === "*") {
    const m = minute.length <= 2 ? minute : minute.split(",")[0];
    return `Every hour at ${formatTime("0", m)}`;
  }

  // Daily at specific time (0 2 * * *, 0 0 * * *, etc.)
  if (dayOfMonth === "*" && month === "*" && dayOfWeek === "*" && !minute.includes("*") && !hour.includes("*")) {
    const timeStr = formatTime(hour, minute);
    return `Daily at ${timeStr}`;
  }

  // Weekly: dayOfWeek is set (e.g. 0 6 * * 1 = Monday 6:00 AM)
  if (dayOfMonth === "*" && month === "*" && dayOfWeek !== "*") {
    const dayNum = parseInt(dayOfWeek, 10);
    if (!Number.isNaN(dayNum) && dayNum >= 0 && dayNum <= 6) {
      const timeStr = formatTime(hour, minute);
      return `Weekly on ${dayNames[dayNum]} at ${timeStr}`;
    }
  }

  return schedule;
}
