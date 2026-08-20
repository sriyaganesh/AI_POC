"""
Agentic AI entry point: an OpenAI-powered LangChain agent that can query
incidents from a ServiceNow PDI via tools.

Run interactively:
    python -m servicenow_agent.agent

Run a single instruction:
    python -m servicenow_agent.agent "List the 5 most recent open incidents"
"""

from __future__ import annotations

import os
import sys

from dotenv import load_dotenv
from langchain.agents import create_agent
from langchain_openai import ChatOpenAI

from .tools import TOOLS


SYSTEM_PROMPT = """
You are a ServiceNow operations assistant with access to tools that query
incidents from a connected ServiceNow PDI (Personal Developer Instance).

Guidelines:

- Use the ServiceNow tools whenever the user asks about incidents,
  tickets, or issues.
- Never invent incident information.
- If the user asks for incidents without specifying a number, return the
  10 most recent incidents.
- If the user specifies a number, return that number.
- If the user asks for open incidents, filter for open incidents.
- If the user asks for a specific incident number, retrieve that incident.
- Summarize results clearly using a table or bullet list.
- Include useful fields such as incident number, short description,
  state, priority, and creation time when available.
- If the ServiceNow tool reports an authentication, configuration,
  connection, or API error, explain the error clearly and suggest a
  practical fix.
- Do not guess or fabricate ServiceNow data.
"""


def build_agent():
    """
    Build and return the LangChain ServiceNow agent.
    """

    load_dotenv()

    if not os.getenv("OPENAI_API_KEY"):
        raise RuntimeError(
            "OPENAI_API_KEY is not set. "
            "Add it to your .env file or environment."
        )

    llm = ChatOpenAI(
        model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
        temperature=0,
    )

    agent = create_agent(
        model=llm,
        tools=TOOLS,
        system_prompt=SYSTEM_PROMPT,
    )

    return agent


def run(instruction: str) -> str:
    """
    Run a single instruction through the agent.
    """

    agent = build_agent()

    result = agent.invoke(
        {
            "messages": [
                {
                    "role": "user",
                    "content": instruction,
                }
            ]
        }
    )

    messages = result.get("messages", [])

    if not messages:
        return "The agent returned no response."

    # The final AI message is normally the last message.
    final_message = messages[-1]

    return final_message.content


def main() -> None:
    """
    CLI entry point.
    """

    if len(sys.argv) > 1:
        # Single-shot mode:
        #
        # python -m servicenow_agent.agent \
        #     "List the 5 most recent open incidents"

        instruction = " ".join(sys.argv[1:])

        print(run(instruction))

        return

    # Interactive mode
    agent = build_agent()

    print("ServiceNow Incident Agent")
    print("Type 'exit' or 'quit' to stop.")

    while True:

        try:
            user_input = input("\nYou: ").strip()

        except (EOFError, KeyboardInterrupt):
            print()
            break

        if not user_input:
            continue

        if user_input.lower() in {"exit", "quit"}:
            break

        try:

            result = agent.invoke(
                {
                    "messages": [
                        {
                            "role": "user",
                            "content": user_input,
                        }
                    ]
                }
            )

            messages = result.get("messages", [])

            if messages:
                final_message = messages[-1]
                print(f"\nAgent: {final_message.content}")
            else:
                print("\nAgent: No response returned.")

        except Exception as exc:

            print(f"\nAgent error: {exc}")


if __name__ == "__main__":
    main()
