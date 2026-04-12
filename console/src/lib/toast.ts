/**
 * Toast helpers wrapping `sonner` (UX-04, Phase 137).
 *
 * Consistent placement (bottom-right) and duration across the console. Success
 * toasts auto-dismiss in 3s; errors linger for 5s so operators can read the
 * cause. Use from mutation paths in `lib/api.ts` — do NOT call from read paths
 * (they'd spam the UI during polling / auto-refresh).
 */

import { toast } from "sonner";

export function toastSuccess(message: string): void {
  toast.success(message, {
    duration: 3000,
    position: "bottom-right",
  });
}

export function toastError(message: string): void {
  toast.error(message, {
    duration: 5000,
    position: "bottom-right",
  });
}

export function toastInfo(message: string): void {
  toast(message, { duration: 3000, position: "bottom-right" });
}
