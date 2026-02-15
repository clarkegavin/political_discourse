# Technical Decision Log:
*This document logs the key technical decisions made regarding the analysis of parliamentary questions in the context of the thesis.*

## Decision: Why choose BERTopic?
*date: 14/02/2026*
### Reason:
* BERTopic is a powerful topic modeling technique that allows for the extraction of coherent topics from large text corpora. It is particularly effective in handling short texts, which is relevant for analyzing parliamentary questions. Additionally, BERTopic provides a way to visualize the topics and their relationships, which can enhance the interpretability of the results.
### Alternative considered:
* Other topic modeling techniques such as Latent Dirichlet Allocation (LDA) or Non-negative Matrix Factorization (NMF)
### Alternative Rejected because:
* LDA and NMF may not perform as well with short texts and may not provide the same level of interpretability and visualization options as BERTopic.
### Supporting literature:
TODO: add supporting literature