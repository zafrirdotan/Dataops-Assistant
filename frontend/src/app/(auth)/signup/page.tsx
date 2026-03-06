import { SignupForm } from "@/components/auth/signup-form";
import { PipelineFlowDiagram } from "@/components/auth/pipeline-flow-diagram";

export default function SignupPage() {
  return (
    <div className="grid min-h-screen lg:grid-cols-2">
      <div className="flex flex-col justify-center gap-8 px-6 py-12 lg:px-12 lg:py-16">
        <div className="space-y-3">
          <h1 className="text-3xl font-bold tracking-tight text-foreground lg:text-4xl">
            DataOps Assistant
          </h1>
          <p className="max-w-md text-muted-foreground lg:text-base">
            Turn natural language into ETL pipelines. Connect CSV, PostgreSQL,
            and APIs to CSV, Parquet, SQLite, or PostgreSQL — powered by an AI
            pipeline engine.
          </p>
        </div>
        <PipelineFlowDiagram />
      </div>
      <div className="flex items-center justify-center p-4 lg:p-8">
        <SignupForm />
      </div>
    </div>
  );
}
