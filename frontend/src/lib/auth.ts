import { AuthResponse, LoginRequest, SignupRequest, User } from "@/types/auth";

const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

export const authService = {
  async login(
    credentials: LoginRequest,
  ): Promise<{ message: string; user: { username: string; email: string } }> {
    const response = await fetch(`${API_URL}/api/auth/login`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      credentials: "include", // Important: include cookies
      body: JSON.stringify(credentials),
    });

    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.detail || "Login failed");
    }

    return response.json();
  },

  async signup(userData: SignupRequest): Promise<User> {
    const response = await fetch(`${API_URL}/api/auth/signup`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      credentials: "include",
      body: JSON.stringify(userData),
    });

    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.detail || "Signup failed");
    }

    return response.json();
  },

  async getCurrentUser(): Promise<User> {
    const response = await fetch(`${API_URL}/api/auth/me`, {
      credentials: "include", // Important: send cookies
    });

    if (!response.ok) {
      if (response.status === 401) {
        throw new Error("Unauthorized");
      }
      throw new Error("Failed to fetch user");
    }

    return response.json();
  },

  async refreshToken(): Promise<{ message: string } | null> {
    try {
      const response = await fetch(`${API_URL}/api/auth/refresh`, {
        method: "POST",
        credentials: "include", // Important: send cookies
      });

      if (!response.ok) {
        return null;
      }

      return response.json();
    } catch (error) {
      return null;
    }
  },

  isAuthenticated(): boolean {
    // With httpOnly cookies, we can't check client-side
    // We need to try fetching the current user
    return true; // Will be validated on server
  },

  async logout() {
    try {
      await fetch(`${API_URL}/api/auth/logout`, {
        method: "POST",
        credentials: "include", // Important: send cookies to clear them
      });
    } catch (error) {
      console.error("Logout error:", error);
    }
  },
};
