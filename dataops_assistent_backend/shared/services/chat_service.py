import json
import logging
import asyncio
from typing import AsyncGenerator, Optional
import uuid

from sqlalchemy import select, update, or_

from shared.services.llm_service import LLMService
from pipeline_builder.guards.guards_service import GuardsService
from pipeline_builder import PipelineBuilderService
from shared.services.storage_service import MinioStorage
from shared.services.database_service import get_database_service
from shared.models.chat_orm import Chat, ChatMessage

from shared.utils.spinner_utils import run_step_with_spinner
logger = logging.getLogger("dataops")

class ChatService:
    def __init__(self):
        self.logger = logger
        self.llm_service = LLMService()
        self.guards_service = GuardsService(log=self.logger)
        self.pipeline_builder_service = PipelineBuilderService()
        self.storage_service = MinioStorage()
        self.db_service = get_database_service()

    async def process_message(self, raw_message: str, fast: bool = False, mode: str = "chat", run_after_deploy: bool = False) -> dict:
        """
        Process the user message, validate it, and get a response from the LLM.
        """

        # Step 1: Run guards on input
        guard_result, guard_error = await self._run_step(
            "Validating request...",
            0,
            self.run_guards_on_input,
            raw_message,
            mode=mode
        )
        if guard_error:
            self.logger.error(f"Error during input guards: {guard_error}")
            return {"guard_decision": "block", "error": str(guard_error)}

        if guard_result["guard_decision"] == "block":
            self.logger.warning("Input blocked by guards.")
            return guard_result

        build_spec = await self.pipeline_builder_service.build_pipeline(guard_result["cleaned_input"], fast=fast, mode=mode, run_after_deploy=run_after_deploy)

        return {
            "guard_decision": "allow",
            "build_spec": build_spec
        }

    async def process_message_stream(
        self,
        raw_message: str = None,
        messages: list[dict] = None,
        fast: bool = False,
        run_after_deploy: bool = False
    ) -> AsyncGenerator[dict, None]:
        """
        Stream processing events for the user message and pipeline build steps.

        Args:
            raw_message: Single message (for backwards compatibility)
            messages: Full conversation history in OpenAI format [{"role": "user/assistant", "content": "..."}]
            fast: Fast mode flag
            run_after_deploy: Whether to run after deployment
        """
        # Build messages array from either messages or raw_message
        if messages is None:
            if raw_message is None:
                raise ValueError("Either raw_message or messages must be provided")
            messages = [{"role": "user", "content": raw_message}]

        # Extract the latest user message for guards
        latest_user_message = None
        for msg in reversed(messages):
            if msg.get("role") == "user":
                latest_user_message = msg.get("content", "")
                break

        if not latest_user_message:
            raise ValueError("No user message found in conversation history")

        system_prompt = (
            "You are a data engineering assistant. "
            "Check if the user provided:\n"
            "- Data source (csv file/files, Postgres DB table)\n"
            "- Pipeline name (optional)\n"
            "- Data destination (type & name)\n"
            "- Transformations (optional if not provided dont ask)\n"
            "- Schedule\n\n"
            "If any are missing, briefly ask for them. "
            "Optionally ask if they want to add a pipeline name. "
            "Be concise.\n\n"
            "You can only ask for missing information."
            "Once you have all required details, call the build_pipeline tool with a synthesis of the user's requirements. "
            "Do NOT repeat the details back to the user. "
            "Do NOT ask for confirmation. "
        )

        # Define the build_pipeline tool
        tools = [
            {
                "type": "function",
                "function": {
                    "name": "build_pipeline",
                    "description": "Build a data pipeline based on user requirements. Call this once you have all required information: data source, destination, transformations, and schedule.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "user_input": {
                                "type": "string",
                                "description": "The complete user requirements for the pipeline including source, destination, transformations, and schedule"
                            }
                        },
                        "required": ["user_input"]
                    }
                }
            }
        ]

        self.logger.info("Starting LLM guidance with tool support...")
        synthesized_user_input = None

        async for delta in self.llm_service.stream_response_with_tools(
            system_prompt=system_prompt,
            messages=messages,
            tools=tools
        ):
            self.logger.debug(f"LLM delta: {delta}")

            # Check if it's a tool call
            if delta.get("type") == "tool_call" and delta.get("tool_name") == "build_pipeline":
                # Extract the synthesized user input from the entire conversation
                synthesized_user_input = delta.get("arguments", {}).get("user_input", latest_user_message)
                yield {"event": "llm", "data": {"delta": "\n\nBuilding pipeline...\n"}}

                # Execute the build_pipeline tool
                async for event in self._execute_build_pipeline_tool(
                    synthesized_user_input,
                    fast=fast,
                    run_after_deploy=run_after_deploy
                ):
                    yield event
                return
            else:
                yield {"event": "llm", "data": {"delta": delta.get("delta", "")}}
                if delta.get("done"):
                    return

    async def _execute_build_pipeline_tool(
        self,
        synthesized_user_input: str,
        fast: bool = False,
        run_after_deploy: bool = False
    ) -> AsyncGenerator[dict, None]:
        """
        Execute the build_pipeline tool: validate input and build the pipeline.

        Args:
            synthesized_user_input: The synthesized user input from LLM
            fast: Fast mode flag
            run_after_deploy: Whether to run after deployment

        Yields:
            Event dictionaries for validation and pipeline building
        """
        # Validate input using GuardsService - it handles all events
        guard_result = None
        async for event in self.guards_service.validate_with_events(
            synthesized_user_input,
            step_name="validate_request",
            step_number=0
        ):
            # Capture validation result for pipeline building
            if event.get("event") == "validation_result":
                guard_result = event.get("data")
            else:
                # Relay all other events (step, llm, guard, final)
                yield event

            # If validation failed, guard service already sent final event
            if event.get("event") == "final":
                return

        # If we don't have a guard_result, validation failed
        if not guard_result:
            return

        # Build the pipeline
        queue: asyncio.Queue = asyncio.Queue()

        async def on_event(payload: dict):
            await queue.put(payload)

        build_task = asyncio.create_task(
            self.pipeline_builder_service.build_pipeline(
                guard_result["cleaned_input"],
                fast=fast,
                mode="chat",
                run_after_deploy=run_after_deploy,
                event_callback=on_event
            )
        )

        while True:
            if build_task.done() and queue.empty():
                break
            try:
                event = await asyncio.wait_for(queue.get(), timeout=0.2)
                yield event
            except asyncio.TimeoutError:
                continue

        build_spec = await build_task
        pipeline_code = build_spec.get("pipeline_code")
        if build_spec.get("error") or build_spec.get("success") is False:
            yield {
                "event": "final",
                "data": {
                    "success": False,
                    "error": build_spec.get("error") or build_spec.get("details"),
                    "pipeline_id": build_spec.get("pipeline_id"),
                    "pipeline_code": pipeline_code,
                    "build_spec": build_spec,
                },
            }
            return
        yield {
            "event": "final",
            "data": {
                "success": True,
                "pipeline_id": build_spec.get("pipeline_id"),
                "pipeline_code": pipeline_code,
                "build_spec": build_spec,
            },
        }

    @staticmethod
    def _summary_for_name(text: str, max_len: int = 60) -> str:
        """Derive a short name from the first user message (fallback, no LLM)."""
        if not text or not isinstance(text, str):
            return "New chat"
        one_line = " ".join(text.split()).strip()
        if len(one_line) <= max_len:
            return one_line or "New chat"
        return one_line[: max_len - 3].rstrip() + "..."

    async def _get_llm_summary_for_name(self, text: str, max_len: int = 60) -> Optional[str]:
        """Ask the LLM for a short phrase suitable as a chat title. Returns None on failure."""
        if not text or not isinstance(text, str):
            return None
        prompt = (
            f"Summarize this in one short phrase suitable for a chat title (max {max_len} characters). "
            "Reply with only the phrase, no quotes or punctuation at the end."
        )
        result = await self.llm_service.complete_one_phrase(prompt, text, max_tokens=80)
        if not result:
            return None
        one_line = " ".join(result.split()).strip()
        if len(one_line) > max_len:
            one_line = one_line[: max_len - 3].rstrip() + "..."
        return one_line or None

    async def _get_unique_chat_name(self, session, base_name: str) -> str:
        """
        Return base_name if unique; otherwise base_name + " 2", " 3", etc.
        Queries existing chat names that equal base_name or match "base_name N".
        """
        if not base_name:
            return "New chat"
        prefix = base_name + " "
        stmt = select(Chat.name).where(
            or_(
                Chat.name == base_name,
                Chat.name.startswith(prefix, autoescape=True),
            )
        )
        result = await session.execute(stmt)
        existing = [row[0] for row in result.all()]
        if not existing:
            return base_name
        used = set(existing)
        if base_name not in used:
            return base_name
        suffix = 2
        while f"{base_name} {suffix}" in used:
            suffix += 1
        return f"{base_name} {suffix}"

    async def create_new_chat(self) -> Optional[str]:
        """
        Create a new chat record in the database using ORM.
        Generates a new UUID for the chat.

        Returns:
            The chat_id as string if successful, None otherwise
        """
        try:
            chat_id = uuid.uuid4()
            async with self.db_service.AsyncSessionLocal() as session:
                new_chat = Chat(id=chat_id)
                session.add(new_chat)
                await session.commit()
            return str(chat_id)
        except Exception as e:
            self.logger.error(f"Failed to create chat: {e}")
            return None

    async def process_chat_stream_with_persistence(
        self,
        chat_id: Optional[str] = None,
        raw_message: Optional[str] = None,
        messages: Optional[list[dict]] = None,
        fast: bool = False,
        run_after_deploy: bool = False
    ) -> AsyncGenerator[dict, None]:
        """
        Complete streaming workflow: create chat if needed, process messages,
        accumulate state, and save to database.

        This method handles all business logic for streaming chat interactions,
        keeping the route layer thin and focused on HTTP/SSE concerns.

        Args:
            chat_id: Optional existing chat ID. If None, creates a new chat.
            raw_message: Single message for backwards compatibility
            messages: Full conversation history
            fast: Fast mode flag
            run_after_deploy: Whether to run pipeline after deployment

        Yields:
            Event dictionaries ready for SSE formatting
        """
        # Create chat if needed
        chat_just_created = False
        if not chat_id:
            chat_id = await self.create_new_chat()
            if chat_id:
                chat_just_created = True
                yield {"event": "chat_created", "data": {"chat_id": chat_id, "name": "Generating..."}}

        # If chat_id provided but no messages, fetch full history from DB
        if chat_id and not messages:
            db_messages = await self.get_chat_history(chat_id)
            messages = [{"role": msg["role"], "content": msg["content"]}
                       for msg in db_messages]
            # Add new message to conversation
            if raw_message:
                messages.append({"role": "user", "content": raw_message})

        # State accumulation
        pipeline_id = None
        pipeline_code = None
        assistant_response = ""
        build_steps = {}
        saw_build_final = False
        user_message = raw_message or (messages[-1]["content"] if messages else "")

        # Save user message immediately if we have a chat_id
        if chat_id and user_message:
            async with self.db_service.AsyncSessionLocal() as session:
                await self.save_user_message(chat_id, user_message, session=session)
                if chat_just_created:
                    await self.update_chat_name(chat_id, "Generating...", session=session)
                await session.commit()

        # For new chats: compute LLM summary + unique name, then update and emit chat_name_updated
        if chat_just_created and chat_id and user_message:
            base_name = await self._get_llm_summary_for_name(user_message)
            if not base_name:
                base_name = self._summary_for_name(user_message)
            async with self.db_service.AsyncSessionLocal() as session:
                unique_name = await self._get_unique_chat_name(session, base_name)
                await self.update_chat_name(chat_id, unique_name, session=session)
                await session.commit()
            yield {"event": "chat_name_updated", "data": {"chat_id": chat_id, "name": unique_name}}

        # Stream events and accumulate state
        async for event in self.process_message_stream(
            raw_message=raw_message,
            messages=messages,
            fast=fast,
            run_after_deploy=run_after_deploy
        ):
            event_type = event.get("event")
            event_data = event.get("data", {})

            match event_type:
                case "final":
                    saw_build_final = True
                    if event_data.get("pipeline_id"):
                        pipeline_id = event_data["pipeline_id"]
                    if event_data.get("pipeline_code") is not None:
                        pipeline_code = event_data["pipeline_code"]
                case "code_generated":
                    if event_data.get("pipeline_code"):
                        pipeline_code = event_data["pipeline_code"]
                case "llm":
                    assistant_response += event_data.get("delta", "")
                case "step":
                    status = event_data.get("status")
                    if status in ["completed", "error"]:
                        step_key = f"{event_data.get('step_number')}_{event_data.get('step_name')}"
                        build_steps[step_key] = {
                            "step_number": event_data.get("step_number"),
                            "step_name": event_data.get("step_name"),
                            "message": event_data.get("message"),
                            "status": status,
                            "error": event_data.get("error"),
                        }

            yield event

        # Persist: assistant message, then steps+code only when build reached terminal state
        if chat_id and assistant_response:
            async with self.db_service.AsyncSessionLocal() as session:
                await self.save_assistant_message(
                    chat_id=chat_id,
                    content=assistant_response,
                    session=session
                )
                if saw_build_final and build_steps:
                    steps_list = sorted(build_steps.values(), key=lambda x: x["step_number"])
                    await self.save_steps_message(
                        chat_id=chat_id,
                        pipeline_id=pipeline_id,
                        pipeline_code=pipeline_code,
                        steps=steps_list,
                        session=session
                    )
                if pipeline_id:
                    await self.update_chat_with_pipeline(chat_id, pipeline_id, session=session)
                await session.commit()

    async def save_chat_interaction(
        self,
        chat_id: str,
        user_message: str,
        assistant_response: str,
        pipeline_id: Optional[str] = None,
        pipeline_code: Optional[str] = None,
        build_steps: Optional[dict] = None
    ) -> bool:
        """
        Save complete chat interaction (user message, assistant response, and chat update)
        in a single database transaction by reusing existing methods.

        Args:
            chat_id: The chat identifier
            user_message: The user's message content
            assistant_response: The assistant's response content
            pipeline_id: Optional pipeline identifier
            pipeline_code: Optional pipeline code
            build_steps: Optional dict of build steps

        Returns:
            True if successful, False otherwise
        """
        try:
            async with self.db_service.AsyncSessionLocal() as session:

                await self.save_user_message(chat_id, user_message, session=session)

                if assistant_response:
                    # Convert dict to sorted list by step_number
                    steps_list = sorted(build_steps.values(), key=lambda x: x["step_number"]) if build_steps else []

                    await self.save_assistant_message_with_steps(
                        chat_id=chat_id,
                        content=assistant_response,
                        pipeline_id=pipeline_id,
                        pipeline_code=pipeline_code,
                        steps=steps_list,
                        session=session
                    )

                # 3. Update chat with pipeline_id if provided
                if pipeline_id:
                    await self.update_chat_with_pipeline(chat_id, pipeline_id, session=session)

                # Commit all changes in one transaction
                await session.commit()
                return True
        except Exception as e:
            self.logger.error(f"Failed to save chat interaction: {e}")
            return False

    async def save_user_message(self, chat_id: str, content: str, session) -> bool:
        """
        Save a user message to the database using ORM.

        Args:
            chat_id: The chat identifier
            content: The message content
            session: Database session (caller manages lifecycle and commit)

        Returns:
            True if successful, False otherwise
        """
        try:
            new_message = ChatMessage(
                chat_id=chat_id,
                role="user",
                content=content
            )
            session.add(new_message)
            return True
        except Exception as e:
            self.logger.error(f"Failed to save user message: {e}")
            return False

    async def save_assistant_message(
        self,
        chat_id: str,
        content: str,
        session
    ) -> bool:
        """
        Save a simple assistant message without extra metadata using ORM.

        Args:
            chat_id: The chat identifier
            content: The assistant's message content
            session: Database session (caller manages lifecycle and commit)

        Returns:
            True if successful, False otherwise
        """
        try:
            new_message = ChatMessage(
                chat_id=chat_id,
                role="assistant",
                content=content
            )
            session.add(new_message)
            return True
        except Exception as e:
            self.logger.error(f"Failed to save assistant message: {e}")
            return False

    async def save_steps_message(
        self,
        chat_id: str,
        session,
        pipeline_id: Optional[str] = None,
        pipeline_code: Optional[str] = None,
        steps: Optional[list] = None
    ) -> bool:
        """
        Save pipeline steps as a separate system message with minimal content.
        This separates step tracking from conversational content.

        Args:
            chat_id: The chat identifier
            session: Database session (caller manages lifecycle and commit)
            pipeline_id: Optional pipeline identifier
            pipeline_code: Optional pipeline code
            steps: Optional list of build steps

        Returns:
            True if successful, False otherwise
        """
        extra_data = {
            "type": "steps",
            "pipeline_id": pipeline_id,
            "pipeline_code": pipeline_code,
            "steps": steps or []
        }

        try:
            new_message = ChatMessage(
                chat_id=chat_id,
                role="system",
                content="",  # Empty content, steps are in extra_data
                extra_data=extra_data
            )
            session.add(new_message)
            return True
        except Exception as e:
            self.logger.error(f"Failed to save steps message: {e}")
            return False

    async def save_assistant_message_with_steps(
        self,
        chat_id: str,
        content: str,
        session,
        pipeline_id: Optional[str] = None,
        pipeline_code: Optional[str] = None,
        steps: Optional[list] = None
    ) -> bool:
        """
        Save an assistant message with optional pipeline steps and metadata using ORM.

        Args:
            chat_id: The chat identifier
            content: The assistant's message content
            session: Database session (caller manages lifecycle and commit)
            pipeline_id: Optional pipeline identifier
            pipeline_code: Optional pipeline code
            steps: Optional list of build steps

        Returns:
            True if successful, False otherwise
        """
        extra_data = None
        if steps or pipeline_code:
            extra_data = {
                "type": "steps",
                "pipeline_id": pipeline_id,
                "pipeline_code": pipeline_code,
                "steps": steps
            }

        try:
            new_message = ChatMessage(
                chat_id=chat_id,
                role="assistant",
                content=content,
                extra_data=extra_data
            )
            session.add(new_message)
            return True
        except Exception as e:
            self.logger.error(f"Failed to save assistant message: {e}")
            return False

    async def update_chat_with_pipeline(self, chat_id: str, pipeline_id: str, session) -> bool:
        """
        Update a chat record with the associated pipeline ID using ORM.

        Args:
            chat_id: The chat identifier
            pipeline_id: The pipeline identifier to associate
            session: Database session (caller manages lifecycle and commit)

        Returns:
            True if successful, False otherwise
        """
        try:
            stmt = (
                update(Chat)
                .where(Chat.id == chat_id)
                .values(pipeline_id=pipeline_id)
            )
            await session.execute(stmt)
            return True
        except Exception as e:
            self.logger.error(f"Failed to update chat with pipeline: {e}")
            return False

    async def update_chat_name(self, chat_id: str, name: str, session) -> bool:
        """
        Update a chat record with the given name (e.g. from first user message summary).

        Args:
            chat_id: The chat identifier
            name: The display name for the chat
            session: Database session (caller manages lifecycle and commit)

        Returns:
            True if successful, False otherwise
        """
        try:
            stmt = (
                update(Chat)
                .where(Chat.id == chat_id)
                .values(name=name)
            )
            await session.execute(stmt)
            return True
        except Exception as e:
            self.logger.error(f"Failed to update chat name: {e}")
            return False

    async def get_chat_history(self, chat_id: str) -> list[dict]:
        """
        Retrieve all messages for a specific chat using ORM.

        Args:
            chat_id: The chat identifier

        Returns:
            List of message dictionaries
        """
        try:
            async with self.db_service.AsyncSessionLocal() as session:
                stmt = (
                    select(ChatMessage)
                    .where(ChatMessage.chat_id == chat_id)
                    .order_by(ChatMessage.created_at.asc())
                )
                result = await session.execute(stmt)
                messages = result.scalars().all()

                return [msg.to_dict() for msg in messages]
        except Exception as e:
            self.logger.error(f"Failed to retrieve chat history: {e}")
            raise

    async def get_chat_id_by_pipeline(self, pipeline_id: str) -> Optional[str]:
        """
        Find the chat ID associated with a specific pipeline using ORM.

        Args:
            pipeline_id: The pipeline identifier

        Returns:
            The chat ID if found, None otherwise
        """
        try:
            async with self.db_service.AsyncSessionLocal() as session:
                stmt = select(Chat).where(Chat.pipeline_id == pipeline_id).limit(1)
                result = await session.execute(stmt)
                chat = result.scalar_one_or_none()

                return chat.id if chat else None
        except Exception as e:
            self.logger.error(f"Failed to retrieve chat by pipeline: {e}")
            raise

    async def list_chats(self) -> list[dict]:
        """
        List all chats ordered by created_at descending.

        Returns:
            List of dicts with id, name, created_at, pipeline_id
        """
        try:
            async with self.db_service.AsyncSessionLocal() as session:
                stmt = (
                    select(Chat)
                    .order_by(Chat.created_at.desc())
                )
                result = await session.execute(stmt)
                chats = result.scalars().all()
                return [
                    {
                        "id": str(c.id),
                        "name": c.name,
                        "created_at": c.created_at.isoformat() if c.created_at else None,
                        "pipeline_id": c.pipeline_id,
                    }
                    for c in chats
                ]
        except Exception as e:
            self.logger.error(f"Failed to list chats: {e}")
            raise

    async def run_guards_on_input(self, raw_message: str) -> dict:
        """
        Run prompt guard analysis and LLM guard checks on the input message.

        Delegates to GuardsService for validation logic.
        """
        return await self.guards_service.validate_input(raw_message)

    async def _run_step(self, step_msg: str, step_number: int, coro, *args, mode="chat", **kwargs):
        """
        Helper to run an async step with optional CLI spinner.
        Returns (result, error). If error is not None, result is None.
        """
        if mode == "cmd":
            return await run_step_with_spinner(step_msg, step_number, coro, *args, **kwargs)
        else:
            try:
                result = await coro(*args, **kwargs)
                return result, None
            except Exception as e:
                return None, e
