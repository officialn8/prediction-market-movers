/**
 * BetterAuth Client
 * Use this in React components for auth operations
 */

import { createAuthClient } from "better-auth/react";
import { polarClient } from "@polar-sh/better-auth/client";

export const authClient = createAuthClient({
  baseURL: process.env.NEXT_PUBLIC_APP_URL || "http://localhost:3000",
  plugins: [
    polarClient(),
  ],
});

// Export individual methods for convenience
export const {
  signIn,
  signUp,
  signOut,
  useSession,
  getSession,
} = authClient;

// Polar-specific exports
export const { 
  checkout,
  customer,
} = authClient;

/**
 * Helper to check if user has a specific tier
 */
export function hasTier(user: { tier?: string } | null, requiredTier: "free" | "pro" | "enterprise"): boolean {
  if (!user) return false;
  
  const tierOrder = { free: 0, pro: 1, enterprise: 2 };
  const userTierLevel = tierOrder[user.tier as keyof typeof tierOrder] ?? 0;
  const requiredTierLevel = tierOrder[requiredTier];
  
  return userTierLevel >= requiredTierLevel;
}

/**
 * Redirect to checkout for a specific tier
 */
export async function upgradeToTier(tier: "pro" | "enterprise") {
  const slug = tier === "pro" ? "pmm-pro" : "pmm-enterprise";
  await authClient.checkout({ products: [slug] });
}

/**
 * Open customer portal for subscription management
 */
export async function openCustomerPortal() {
  await authClient.customer.portal();
}

/**
 * Get current customer state (subscriptions, benefits, etc.)
 */
export async function getCustomerState() {
  const { data } = await authClient.customer.state();
  return data;
}
