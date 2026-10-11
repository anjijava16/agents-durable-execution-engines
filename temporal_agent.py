# No `workflow.unsafe.imports_passed_through()` guard is needed: the plugin
# configures the workflow sandbox to pass the deepagents / LangChain import
# tree through, so workflow files import them like any other module.
from deepagents import create_deep_agent
from temporalio import workflow


@workflow.defn
class HelloWorldAgent:
    @workflow.run
    async def run(self, question: str) -> str:
        agent = create_deep_agent(
            model="anthropic:claude-sonnet-4-5",
            system_prompt="You are a helpful assistant. Answer concisely.",
        )
        result = await agent.ainvoke(
            {"messages": [{"role": "user", "content": question}]}
        )
        return result["messages"][-1].content

