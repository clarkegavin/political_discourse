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


## Decision: Why not use a more traditional topic modeling technique like LDA or NMF?
* date: 15/03/2026*
* ### Reason:
* Traditional topic modeling techniques like LDA and NMF may not be as effective in handling short texts, which is relevant for analyzing parliamentary questions. Additionally, these techniques may not provide the same level of interpretability and visualization options as BERTopic, which can enhance the understanding of the results.
* ### Alternative considered:
* BERTopic
* ### Alternative Rejected because:
* BERTopic is specifically designed to handle short texts and provides better interpretability and visualization options compared to traditional topic modeling techniques.
* ### Supporting literature:
TODO: add supporting literature 


## Decision: Why not use a social sciences technique like STM (Structural Topic Modeling)?
* date: 15/03/2026*
* ### Reason:
* STM is a powerful technique for analyzing text data in the social sciences, but it may not be the best fit for our analysis of parliamentary questions. STM is designed to incorporate metadata and covariates into the topic modeling process, which may not be necessary or relevant for our analysis. Additionally, STM may require more complex modeling and interpretation compared to BERTopic, which is more straightforward and easier to implement for our specific use case.
* My Research Question is not focused on understanding how different covariates (e.g. party affiliation, time period) influence the topics, but rather on identifying the main themes and issues present in the parliamentary questions, which can be effectively achieved using BERTopic.
* ### Alternative considered:
* BERTopic
* ### Alternative Rejected because:
* N/A
* ### Supporting literature:
TODO: add supporting literature

## Decision: Why not use word2vec or other word embedding techniques for topic modeling?
* date: 15/03/2026*
* ### Reason:
* BERTopic adds a more interpretable topic representation layer on top of the clustering of word embeddings, which can provide more coherent and meaningful topics compared to using word embeddings alone. Additionally, BERTopic allows for the visualization of topics and their relationships, which can enhance the interpretability of the results. Using word2vec or other word embedding techniques alone may not provide the same level of interpretability and may require additional steps to extract meaningful topics from the embeddings.
* ### Alternative considered:
* N/A
* ### Alternative Rejected because:
* N/A
* ### Supporting literature:
TODO: add supporting literature