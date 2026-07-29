from sentence_transformers import SentenceTransformer
from transformers import AutoConfig


models = [
    "sentence-transformers/all-MiniLM-L6-v2",
    "sentence-transformers/all-roberta-large-v1",
    "ddore14/RooseBERT-scr-cased"
]

for model_name in models:
    model = SentenceTransformer(model_name)
    print(model_name)
    print("max_seq_length:", model.max_seq_length)
    print()



models = [
    "sentence-transformers/all-MiniLM-L6-v2",
    "sentence-transformers/all-roberta-large-v1",
    "ddore14/RooseBERT-scr-cased"
]

for model_name in models:
    config = AutoConfig.from_pretrained(model_name)

    print(model_name)
    print("max_position_embeddings:", config.max_position_embeddings)
    print()