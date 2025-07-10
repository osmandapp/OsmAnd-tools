from typing import Any, List

from langchain.docstore.document import Document
from langchain.schema import BaseRetriever


# A re-ranking retriever (wrapping a BaseRetriever and a reranker model).
class RerankingRetriever(BaseRetriever):
    base_retriever: BaseRetriever
    reranker_model: Any

    class Config:
        # Allow arbitrary types (like our retriever and reranker model).
        arbitrary_types_allowed = True

    def _get_relevant_documents(self, query: str, **kwargs) -> List[Document]:
        candidate_docs = self.base_retriever.get_relevant_documents(query)
        if not candidate_docs:
            return candidate_docs
        pairs = [(query, doc.page_content) for doc in candidate_docs]
        scores = self.reranker_model.compute_score(pairs, normalize=True)
        doc_score_pairs = list(zip(candidate_docs, scores))
        doc_score_pairs.sort(key=lambda x: x[1], reverse=True)
        for doc, score in doc_score_pairs:
            print(f"{doc.metadata.get('source', 'unknown')} scored: {score:.4f}")

        docs = [doc for doc, score in doc_score_pairs if score >= 0.01]
        if len(docs) == 0:
            docs.append(doc_score_pairs[0][0])

        print(f"Relevant docs count: {len(docs)}", flush=True)
        return docs

    async def _aget_relevant_documents(self, query: str, **kwargs) -> List[Document]:
        from concurrent.futures import ThreadPoolExecutor
        import asyncio
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as pool:
            result = await loop.run_in_executor(pool, self.get_relevant_documents, query)
        return result
