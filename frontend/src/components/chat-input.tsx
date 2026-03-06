import { forwardRef, useImperativeHandle, useRef } from "react";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";

type ChatInputProps = {
  value: string;
  onChange: (value: string) => void;
  onSubmit: () => void;
  disabled?: boolean;
  showStatus?: boolean;
  statusText?: string;
  className?: string;
};

export interface ChatInputHandle {
  focus: () => void;
}

export const ChatInput = forwardRef<ChatInputHandle, ChatInputProps>(
  function ChatInput(
    {
      value,
      onChange,
      onSubmit,
      disabled = false,
      showStatus = false,
      statusText,
      className,
    }: ChatInputProps,
    ref,
  ) {
    const textareaRef = useRef<HTMLTextAreaElement>(null);

    useImperativeHandle(ref, () => ({
      focus: () => {
        textareaRef.current?.focus();
      },
    }));

    const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
      if (e.key === "Enter" && !e.shiftKey) {
        e.preventDefault();
        if (!disabled) {
          onSubmit();
        }
      }
    };

    return (
      <div className={className}>
        <div className="mt-4 flex gap-3 border border-border px-4 py-4 rounded-md">
          <Textarea
            ref={textareaRef}
            placeholder="Describe the pipeline"
            value={value}
            onChange={(e) => onChange(e.target.value)}
            onKeyDown={handleKeyDown}
            rows={4}
            className="border-0 resize-none shadow-none outline-none focus-visible:ring-0 focus-visible:ring-offset-0"
          />
          <div className="flex items-center justify-between">
            <Button onClick={onSubmit} disabled={disabled}>
              Send
            </Button>
          </div>
        </div>
        {showStatus && (
          <div className="mt-2 text-xs text-muted-foreground">
            {statusText ?? "Ready"}
          </div>
        )}
      </div>
    );
  },
);
