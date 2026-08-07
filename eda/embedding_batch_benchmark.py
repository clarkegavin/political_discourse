# eda/embedding_batch_benchmark.py

import os
import time
from pathlib import Path

import pandas as pd
import torch
from sentence_transformers import SentenceTransformer
from transformers import AutoTokenizer

from logs.logger import get_logger
from .base import EDAComponent


class EmbeddingBatchBenchmarkEDA(EDAComponent):
    """
    Benchmarks embedding performance across models and batch sizes.

    Supports optional token-based chunking to replicate production
    BERTopic embedding behaviour.
    """

    def __init__(
        self,
        models=None,
        batch_sizes=None,
        device="cuda",
        sample_size=None,
        output_filename="embedding_batch_benchmark.csv",
        chunking_enabled=False,
        chunk_size=480,
        chunk_overlap=32,
        **kwargs
    ):

        self.logger = get_logger(self.__class__.__name__)

        self.models = models or []

        self.batch_sizes = batch_sizes or [
            32,
            64,
            128,
            256,
            512
        ]

        self.device = device
        self.sample_size = sample_size

        self.output_filename = output_filename

        self.chunking_enabled = chunking_enabled
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap


    def _chunk_texts(
        self,
        texts,
        model_name,
        chunk_size,
        chunk_overlap
    ):
        """
        Token based chunking.

        Uses the tokenizer associated with the embedding model.
        """

        tokenizer = AutoTokenizer.from_pretrained(
            model_name
        )

        chunks = []

        for text in texts:

            tokens = tokenizer.encode(
                text,
                add_special_tokens=False
            )

            if len(tokens) <= chunk_size:
                chunks.append(text)
                continue

            start = 0

            while start < len(tokens):

                end = start + chunk_size

                chunk_tokens = tokens[start:end]

                chunk = tokenizer.decode(
                    chunk_tokens,
                    skip_special_tokens=True
                )

                chunks.append(chunk)

                start += (
                    chunk_size -
                    chunk_overlap
                )

        return chunks


    def run(
        self,
        data,
        target=None,
        text_field=None,
        save_path=None,
        **kwargs
    ):

        # Resolve YAML parameters
        embedding_text_field = kwargs.get(
            "embedding_text_field",
            text_field
        )

        models = kwargs.get(
            "models",
            self.models
        )

        batch_sizes = kwargs.get(
            "batch_sizes",
            self.batch_sizes
        )

        device = kwargs.get(
            "device",
            self.device
        )

        sample_size = kwargs.get(
            "sample_size",
            self.sample_size
        )

        chunking_enabled = kwargs.get(
            "chunking_enabled",
            self.chunking_enabled
        )

        chunk_size = kwargs.get(
            "chunk_size",
            self.chunk_size
        )

        chunk_overlap = kwargs.get(
            "chunk_overlap",
            self.chunk_overlap
        )

        output_filename = kwargs.get(
            "output_filename",
            self.output_filename
        )


        self.logger.info(
            f"Benchmark configuration:"
            f" models={models},"
            f" batch_sizes={batch_sizes},"
            f" device={device},"
            f" chunking={chunking_enabled},"
            f" chunk_size={chunk_size},"
            f" overlap={chunk_overlap}"
        )


        if embedding_text_field not in data.columns:
            raise ValueError(
                f"Embedding text field '{embedding_text_field}' not found"
            )


        texts = (
            data[embedding_text_field]
            .fillna("")
            .astype(str)
            .tolist()
        )


        document_count = len(texts)


        if sample_size:
            texts = texts[:sample_size]


        self.logger.info(
            f"Benchmarking {len(texts)} documents"
        )


        results = []


        for model_name in models:

            self.logger.info(
                f"Evaluating {model_name}"
            )

            if chunking_enabled:

                self.logger.info(
                    f"Applying chunking "
                    f"size={chunk_size}, "
                    f"overlap={chunk_overlap}"
                )

                benchmark_texts = self._chunk_texts(
                    texts,
                    model_name,
                    chunk_size,
                    chunk_overlap
                )

            else:

                benchmark_texts = texts


            chunk_count = len(benchmark_texts)


            self.logger.info(
                f"{model_name}: "
                f"{document_count} documents -> "
                f"{chunk_count} embedding inputs"
            )


            model = SentenceTransformer(
                model_name,
                device=device
            )


            if (
                device == "cuda"
                and torch.cuda.is_available()
            ):

                model.encode(
                    benchmark_texts[:32],
                    batch_size=32,
                    show_progress_bar=False
                )


            for batch_size in batch_sizes:

                if device == "cuda":

                    torch.cuda.empty_cache()
                    torch.cuda.reset_peak_memory_stats()


                start = time.perf_counter()


                embeddings = model.encode(
                    benchmark_texts,
                    batch_size=batch_size,
                    show_progress_bar=False
                )


                elapsed = time.perf_counter() - start


                if device == "cuda":

                    peak_memory = (
                        torch.cuda.max_memory_allocated()
                        /
                        1024**3
                    )

                else:

                    peak_memory = None


                results.append(
                    {
                        "model": model_name,
                        "batch_size": batch_size,
                        "documents": document_count,
                        "embedding_inputs": chunk_count,
                        "embedding_dimensions": embeddings.shape[1],
                        "seconds": round(elapsed, 3),
                        "inputs_per_second": round(
                            chunk_count / elapsed,
                            2
                        ),
                        "peak_gpu_memory_gb": round(
                            peak_memory,
                            3
                        )
                        if peak_memory else None
                    }
                )


                self.logger.info(
                    f"{model_name} "
                    f"batch={batch_size}: "
                    f"{elapsed:.2f}s"
                )


            del model

            if torch.cuda.is_available():
                torch.cuda.empty_cache()


        results_df = pd.DataFrame(results)


        output_path = (
            Path(save_path)
            /
            output_filename
        )


        results_df.to_csv(
            output_path,
            index=False
        )


        self.logger.info(
            f"Saved benchmark results: {output_path}"
        )


        return results_df