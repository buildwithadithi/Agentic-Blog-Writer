from langchain_google_genai import ChatGoogleGenerativeAI

from blog_writer.config import get_google_api_key

llm = ChatGoogleGenerativeAI(
    model="gemini-3.1-flash-lite",
    google_api_key=get_google_api_key(),
    temperature=0,
)
