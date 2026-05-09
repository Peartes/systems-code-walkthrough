import { clsx, type ClassValue } from "clsx"
import { twMerge } from "tailwind-merge"

// shadcn convention: every component imports `cn` from this file. Combines
// conditional class lists (clsx) and de-duplicates conflicting Tailwind
// classes (tailwind-merge).
export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}
