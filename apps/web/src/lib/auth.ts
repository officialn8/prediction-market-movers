/**
 * BetterAuth Server Configuration
 * Handles authentication + Polar billing integration
 */

import { betterAuth } from "better-auth";
import { polar, checkout, portal, webhooks } from "@polar-sh/better-auth";
import { Polar } from "@polar-sh/sdk";
import { Pool } from "pg";

// PostgreSQL connection for BetterAuth
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

// Polar SDK client
const polarClient = new Polar({
  accessToken: process.env.POLAR_ACCESS_TOKEN,
});

export const auth = betterAuth({
  // Database adapter - uses PostgreSQL
  database: pool,
  
  // Base URL for auth routes
  baseURL: process.env.BETTER_AUTH_URL || "http://localhost:3000",
  
  // Email + Password authentication
  emailAndPassword: {
    enabled: true,
    requireEmailVerification: false, // Enable in production
  },
  
  // Social providers
  socialProviders: {
    github: {
      clientId: process.env.GITHUB_CLIENT_ID || "",
      clientSecret: process.env.GITHUB_CLIENT_SECRET || "",
    },
    google: {
      clientId: process.env.GOOGLE_CLIENT_ID || "",
      clientSecret: process.env.GOOGLE_CLIENT_SECRET || "",
    },
  },
  
  // Session configuration
  session: {
    expiresIn: 60 * 60 * 24 * 7, // 7 days
    updateAge: 60 * 60 * 24, // Update session every 24 hours
    cookieCache: {
      enabled: true,
      maxAge: 60 * 5, // 5 minutes cache
    },
  },
  
  // User configuration
  user: {
    additionalFields: {
      tier: {
        type: "string",
        defaultValue: "free",
      },
    },
    deleteUser: {
      enabled: true,
      // Sync deletion with Polar
      afterDelete: async (user) => {
        try {
          const { Polar } = await import("@polar-sh/sdk");
          const polarClient = new Polar({
            accessToken: process.env.POLAR_ACCESS_TOKEN,
          });
          await polarClient.customers.deleteExternal({
            externalId: user.id,
          });
        } catch (e) {
          console.error("Failed to delete Polar customer:", e);
        }
      },
    },
  },
  
  // Plugins
  plugins: [
    // Polar integration
    polar({
      client: polarClient,
      // Auto-create Polar customer on signup
      createCustomerOnSignUp: true,
      // Use plugins for checkout, portal, webhooks
      use: [
        // Checkout plugin
        checkout({
          products: [
            { productId: process.env.POLAR_PRO_PRODUCT_ID!, slug: "pmm-pro" },
            { productId: process.env.POLAR_ENTERPRISE_PRODUCT_ID!, slug: "pmm-enterprise" },
          ],
          successUrl: "/dashboard?checkout=success",
          authenticatedUsersOnly: true,
        }),
        
        // Customer portal for self-service
        portal({
          returnUrl: process.env.BETTER_AUTH_URL || "http://localhost:3000",
        }),
        
        // Webhook handlers
        webhooks({
          secret: process.env.POLAR_WEBHOOK_SECRET!,
          
          // Handle successful subscription
          onSubscriptionCreated: async ({ data }) => {
            console.log("Subscription created:", data.id);
            // Tier upgrade is handled automatically via customer state
          },
          
          onSubscriptionActive: async ({ data }) => {
            console.log("Subscription active:", data.id);
          },
          
          onSubscriptionCanceled: async ({ data }) => {
            console.log("Subscription canceled:", data.id);
          },
          
          onSubscriptionRevoked: async ({ data }) => {
            console.log("Subscription revoked:", data.id);
          },
          
          // Catch-all for debugging
          onPayload: async (payload) => {
            console.log("Polar webhook received:", payload.type);
          },
        }),
      ],
    }),
  ],
});

// Export types
export type Session = typeof auth.$Infer.Session;
export type User = typeof auth.$Infer.Session.user;
