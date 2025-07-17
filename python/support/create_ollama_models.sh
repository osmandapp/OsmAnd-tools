#!/bin/bash -e

# Script to make all Ollama models ready for context length 32K
# Get the list of models from 'ollama list'
models=$(ollama list | awk 'NR > 1 {print $1}')

# Iterate over each model in the list
for model in $models; do
    # Skip any empty lines or irrelevant outputs
    if [ -z "$model" ]; then
        continue
    fi

    # Create a model parameter file for the current model
    safe_model_name="${model//\//_}"
    model_param="model_param_${safe_model_name}"
    echo "FROM ${model}" > $model_param
    echo "PARAMETER num_ctx 32768" >> $model_param
    echo "PARAMETER temperature 0.2" >> $model_param

    # Run the 'ollama create' command with the generated model_param file
    #echo "ollama create ${model}-32k -f $model_param"
    ollama create "${model}-32k" -f $model_param

    # Optionally, clean up the model_param file after use
    rm $model_param
done
