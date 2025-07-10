#!/bin/bash -e
export KNOWLEDGE_SIZE=32768
export MODEL_TEMPERATURE=0.0

export MODEL_NAME=$MODEL
if [[ "$MODEL" == "gemini2flash-\$" ]]; then
	MODEL="or@google/gemini-2.0-flash-001"
elif [[ "$MODEL" == "qwen-plus-\$" ]]; then
	MODEL="or@qwen/qwen-plus"	
elif [[ "$MODEL" != *"@"* ]]; then
	MODEL="ollama@$MODEL"
	if [[ "$MODEL" != *"-32k" ]]; then
		MODEL="$MODEL-32k"
	fi
fi
export MODEL_FULL_NAME=$MODEL


if [[ "$KNOWLEDGE" == "purchase" ]]; then
  export KNOWLEDGE_RAG=false
  export KNOWLEDGE_BASE_DIR=$WEB_PATH/docs/web/main/docs/user
  export KNOWLEDGE_FIXED=purchases/*.md,troubleshooting/setup.md
elif [[ "$KNOWLEDGE" == "rag" ]]; then
  export KNOWLEDGE_RAG=true
  export KNOWLEDGE_BASE_DIR=${WEB_PATH}/rag/documents
  export KNOWLEDGE_FIXED=
  export RAG_RERANKER_MODEL=BAAI/bge-reranker-v2-m3
  export RAG_EMBEDDINGS_MODEL=sentence-transformers/all-MiniLM-L6-v2
  export RAG_TOP_K=32
fi

if [[ "$TICKETS" == "pending" ]]; then
	export ZENDESK_VIEW=62871309
elif [[ "$TICKETS" == "solved" ]]; then
	export ZENDESK_VIEW=62871229
elif [[ "$TICKETS" == "validate" ]]; then
    export ZENDESK_VIEW=24862591609885
else 
    export ZENDESK_VIEW=150931385
fi
export ZENDESK_DOMAIN=osmandhelp
export ZENDESK_USER=osmand.help@gmail.com
export VECTOR_DB_DIR=$WEB_PATH/rag/vector-db

if $CLEAN_LOCAL_KNOWLEDGE; then
	rm -rf $VECTOR_DB_DIR/ || true
	echo "Vector DB is deleted: ${CLEAN_LOCAL_KNOWLEDGE}!"
fi

### Generate report
DATA_FOLDER=$(date +%Y-%m-%d)
mkdir -p $WEB_PATH/reports/$DATA_FOLDER
rm $WEB_PATH/latest || true
ln -s $WEB_PATH/reports/$DATA_FOLDER $WEB_PATH/latest

cd $WEB_PATH/reports/$DATA_FOLDER
python3 $SCRIPT_DIR/support/SummaryReport.py
cp $SCRIPT_DIR/support/_report.html .