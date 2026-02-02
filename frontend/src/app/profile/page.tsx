import { ProtectedRoute } from "@/components/auth/protected-route";
import { UserProfile } from "@/components/auth/user-profile";

export default function ProfilePage() {
  return (
    <ProtectedRoute>
      <div className="min-h-screen bg-gray-50 p-8">
        <div className="max-w-4xl mx-auto">
          <h1 className="text-3xl font-bold mb-8">My Profile</h1>
          <UserProfile />
        </div>
      </div>
    </ProtectedRoute>
  );
}
