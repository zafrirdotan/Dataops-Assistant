"use client";

import { useAuth } from "@/hooks/useAuth";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

export function UserProfile() {
  const { user, logout } = useAuth();

  if (!user) return null;

  return (
    <Card className="w-full max-w-md">
      <CardHeader>
        <CardTitle>User Profile</CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <div>
          <p className="text-sm text-gray-600">Username</p>
          <p className="font-medium">{user.username}</p>
        </div>
        <div>
          <p className="text-sm text-gray-600">Email</p>
          <p className="font-medium">{user.email}</p>
        </div>
        <div>
          <p className="text-sm text-gray-600">Account Status</p>
          <p className="font-medium">
            {user.is_active ? "Active" : "Inactive"}
          </p>
        </div>
        <div>
          <p className="text-sm text-gray-600">Role</p>
          <p className="font-medium">{user.is_superuser ? "Admin" : "User"}</p>
        </div>
        <Button onClick={logout} variant="destructive" className="w-full">
          Logout
        </Button>
      </CardContent>
    </Card>
  );
}
