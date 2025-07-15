import glob
import os
import time

# --- Third-party libraries used for knowledge retrieval ---
from FlagEmbedding import FlagReranker
from langchain_chroma import Chroma
from langchain_community.document_loaders import DirectoryLoader
from langchain_community.document_loaders import UnstructuredFileLoader
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_text_splitters import RecursiveCharacterTextSplitter

from .Ranker import RerankingRetriever

is_rag = os.getenv('KNOWLEDGE_RAG', 'false').lower() == 'true'
base_dir = os.getenv('KNOWLEDGE_BASE_DIR', '')
fixed = os.getenv('KNOWLEDGE_FIXED', '')
vector_db_dir = os.getenv('VECTOR_DB_DIR', '')
rag_reranker_model = os.getenv('RAG_RERANKER_MODEL', "BAAI/bge-reranker-v2-m3")
rag_embeddings_model = os.getenv('RAG_EMBEDDINGS_MODEL', "sentence-transformers/all-MiniLM-L6-v2")
rag_top_k = int(os.getenv('RAG_TOP_K', 32))
knowledge_size = int(os.getenv('KNOWLEDGE_SIZE', 2 * 1024))


def knowledge_type():
    if not is_rag and not fixed:
        return 'none'
    elif is_rag and fixed:
        return 'rag&fixed'
    else:
        return 'rag' if is_rag else 'fixed'


class Knowledge:
    def __init__(self, prompts):
        self.prompts = prompts
        self.embeddings = None

        if fixed:
            self.static_knowledge_context = self._fixed_knowledge_context(fixed.split(','))
        else:
            self.static_knowledge_context = None

    def _load_vector_db(self):
        if self.embeddings is None:
            self.embeddings = HuggingFaceEmbeddings(model_name=rag_embeddings_model)

        if not os.path.exists(vector_db_dir):
            print("Vector database not found. Creating a new one...")
            return self._create_vector_db()

        print(f"Vector database found at {vector_db_dir}. Skipping document processing.", flush=True)
        return Chroma(persist_directory=vector_db_dir, embedding_function=self.embeddings)

    def _create_vector_db(self):
        loader = DirectoryLoader(
            path=base_dir,
            loader_cls=UnstructuredFileLoader,
            glob="**/*.md",
            recursive=True,
            show_progress=True,
            use_multithreading=True,
            loader_kwargs={
                "unstructured_kwargs": {
                    "encoding": "utf-8"
                }
            }
        )
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=knowledge_size, chunk_overlap=0,
            separators=["\n#", "\n##", "\n###", "\n\n", "\n", " "]
        )
        documents = loader.load()
        split_docs = text_splitter.split_documents(documents)

        vector_db = Chroma.from_documents(split_docs, self.embeddings, persist_directory=vector_db_dir)
        print(f"Vector database created and saved to {vector_db_dir}. Documents size: {len(documents)}", flush=True)
        return vector_db

    @staticmethod
    def _fixed_knowledge_context(patterns):
        start_time = time.time()

        matching_files = []
        for pattern in patterns:
            full_pattern = os.path.join(base_dir, pattern)
            matching_files.extend(glob.glob(full_pattern, recursive=True))
        matching_files = list(set(matching_files))

        knowledge = ""
        parts_in_context = 0
        for file_path in matching_files:
            with open(file_path, "r", encoding="utf-8") as file:
                content = file.read()

            if len(knowledge) + len(content) >= knowledge_size:
                break
            print(f"Use fixed knowledge: {file_path}")
            knowledge += f"\n{content}"
            parts_in_context += 1

        print(f"Fixed knowledge length: {len(knowledge)}/{parts_in_context}, "
              f"Retrieving completed: {(time.time() - start_time):.6f} seconds.", flush=True)
        return knowledge

    def _relevance_knowledge_context(self, subject: str, question: str, answer: str):
        start_time = time.time()

        vector_db = self._load_vector_db()
        reranking_retriever = RerankingRetriever(
            base_retriever=vector_db.as_retriever(search_kwargs={"k": rag_top_k}),
            reranker_model=FlagReranker(model_name_or_path=rag_reranker_model))

        knowledge = ""
        parts_in_context = 0
        retrieved_docs = reranking_retriever.get_relevant_documents(f"{subject} {question} {answer}")
        prefix = "<?xml version='1.0' encoding='utf-8'?>"
        for doc in retrieved_docs:
            with open(doc.metadata['source'], "r", encoding="utf-8") as f:
                context = f"\n{f.read()}"
            if len(knowledge) + len(context) >= knowledge_size:
                break
            knowledge += context.replace(prefix, "")
            parts_in_context += 1

        print(f"Knowledge length: {len(knowledge)}/{parts_in_context}, "
              f"Retrieving completed: {(time.time() - start_time):.6f} seconds.", flush=True)
        return knowledge

    def knowledge_context(self, subject: str, question: str, answer: str):
        knowledge = ""
        if is_rag:
            knowledge += f"\n{self._relevance_knowledge_context(subject, question, answer)}"
        if self.static_knowledge_context:
            knowledge += f"\n{self.static_knowledge_context}"

        return knowledge
