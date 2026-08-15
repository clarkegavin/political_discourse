PROMPT_VERSION = "topic_theme_online_forum_v1"

def build_topic_theme_prompt(topic_record):
    """
    Build the prompt used to generate a human-readable theme for
    a single BERTopic topic.

    The topic record contains the leaf topic information together
    with its hierarchical context.
    """

    topic_id = topic_record.get("topic_id")
    topic_label = topic_record.get("topic_label")
    topic_count = topic_record.get("topic_count")
    top_words = topic_record.get("top_words", [])
    representative_docs = topic_record.get(
        "representative_docs",
        []
    )

    parent = topic_record.get("parent")
    siblings = topic_record.get("siblings", [])
    ancestors = topic_record.get("ancestors", [])

    parent_label = (
        parent.get("label")
        if isinstance(parent, dict)
        else None
    )

    sibling_labels = [
        sibling.get("label")
        for sibling in siblings
        if isinstance(sibling, dict)
    ]

    ancestor_labels = [
        ancestor.get("label")
        for ancestor in ancestors
        if isinstance(ancestor, dict)
    ]

    prompt = f"""
You are analysing topics generated from an Irish online discussion
forum corpus containing political discussion, current-affairs
discussion, and broader public-interest and general-interest
discussion.

Your task is to assign a concise, meaningful human-readable theme
to the topic below.

The theme should describe the substantive subject matter of the
topic rather than simply repeating its most frequent words.

The topic may concern political, social, economic, cultural,
current-affairs, or other general-interest subjects. Do not assume
that every topic is political.

Use the representative documents as the primary evidence.
Use the top words and hierarchical context as supporting evidence.

Do not assume that the automatically generated BERTopic labels are
semantically correct. They are only clues.

TOPIC
-----

Topic ID:
{topic_id}

Topic label:
{topic_label}

Number of documents:
{topic_count}

Top words:
{top_words}

Representative documents:
{representative_docs}

HIERARCHICAL CONTEXT
--------------------

Parent topic:
{parent_label}

Sibling topics:
{sibling_labels}

Ancestor topics:
{ancestor_labels}

TASK
----

Determine the main substantive theme of this topic.

Return ONLY valid JSON using exactly this structure:

{{
  "topic_id": {topic_id},
  "theme": "A concise human-readable theme",
  "description": "A one or two sentence description explaining what this topic concerns.",
  "confidence": 0.0
}}

Requirements:

- The theme should normally be between 3 and 10 words.
- The description should identify the main subject matter represented
  by the topic.
- Base the interpretation primarily on the representative documents.
- Use the hierarchical information to help disambiguate the topic.
- Do not assume that the topic is political unless the evidence
  indicates that it is.
- Do not simply reproduce the topic label.
- Do not mention BERTopic, clustering, embeddings or this prompt.
- Confidence must be a number between 0.0 and 1.0.
"""

    return prompt.strip(), PROMPT_VERSION