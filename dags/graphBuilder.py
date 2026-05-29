# -*- coding: utf-8 -*-
import spacy
from nltk.stem import WordNetLemmatizer

nlp = spacy.load("en_core_web_lg")
lemmatizer = WordNetLemmatizer()


def _get_syntactic_relations(doc):
    chunks = []
    relations = []
    subjects = {}
    conjunctions = {}
    chunk_to_text = {}

    for chunk in doc.noun_chunks:
        normalized = ' '.join([
            lemmatizer.lemmatize(token.text.lower(), pos='n')
            for token in chunk
            if token.text.lower() not in ['the', 'a', 'an']
        ])
        chunks.append((chunk.start_char, chunk.end_char, chunk, normalized, chunk.root.head, chunk.root.dep_))
        chunk_to_text[chunk.root] = normalized

    for token in doc:
        if token.dep_ == "conj" and token.head in chunk_to_text:
            head_text = chunk_to_text[token.head]
            conj_text = chunk_to_text.get(token)
            if head_text and conj_text:
                conjunctions.setdefault(head_text, []).append(conj_text)

    for token in doc:
        if token.dep_ == "conj" and token.head.pos_ == "NOUN":
            head_text = chunk_to_text.get(token.head, token.head.text.lower())
            conj_text = chunk_to_text.get(token, token.text.lower())
            relations.append((head_text, "and", conj_text))

    for chunk in chunks:
        if chunk[5] == 'nsubj':
            subject_text = chunk_to_text.get(chunk[2].root, chunk[3])
            subjects.setdefault(chunk[4], []).append(subject_text)
            if subject_text in conjunctions:
                subjects[chunk[4]].extend(conjunctions[subject_text])

    for i, chunk in enumerate(chunks):
        if chunk[4].pos_ == 'VERB' and chunk[5] != 'nsubj':
            subject_list = subjects.get(chunk[4], [])
            object_text = chunk_to_text.get(chunk[2].root, chunk[3])
            for subject in subject_list:
                relations.append((subject, chunk[4].text, object_text))
                if object_text in conjunctions:
                    for conj in conjunctions[object_text]:
                        relations.append((subject, chunk[4].text, conj))

        if chunk[4].pos_ == 'VERB' and i + 1 < len(chunks):
            next_chunk = chunks[i + 1]
            if next_chunk[4].pos_ == 'ADP':
                subject_list = subjects.get(chunk[4], [])
                relation_text = f"{chunk[4].text} {next_chunk[4].text}"
                object_text = chunk_to_text.get(next_chunk[2].root, next_chunk[3])
                for subject in subject_list:
                    relations.append((subject, relation_text, object_text))

    for token in doc:
        if token.dep_ == "prep" and token.head.pos_ == "NOUN":
            prep_text = token.text
            object_text = None
            for child in token.children:
                if child.dep_ == "pobj":
                    object_text = chunk_to_text.get(child, child.text.lower())
            if object_text:
                head_text = chunk_to_text.get(token.head, token.head.text.lower())
                subject_list = [head_text]
                if head_text in conjunctions:
                    subject_list.extend(conjunctions[head_text])
                for subject in subject_list:
                    relations.append((subject, prep_text, object_text))

    return relations


def extract_graph_edges(text):
    """Extract (agent_1, agent_2, meaning) triples from text using spaCy NLP.

    Returns list of tuples, no self-loops, no empty strings.
    """
    doc = nlp(text)
    relations = _get_syntactic_relations(doc)
    return [
        (a1, a2, meaning)
        for a1, a2, meaning in relations
        if a1 and a2 and a1 != a2
    ]


def merge_graph(graph, new_edges):
    """Merge new_edges into graph dict, incrementing weight on duplicates.

    Args:
        graph: dict with keys 'nodes' (list of str) and 'edges'
               (list of dicts with agent_1, agent_2, meaning, weight)
        new_edges: list of (agent_1, agent_2, meaning) tuples

    Returns:
        Updated graph dict (mutates and returns the same dict).
    """
    edge_index = {
        (e['agent_1'], e['agent_2'], e['meaning']): i
        for i, e in enumerate(graph['edges'])
    }
    nodes = set(graph['nodes'])

    for agent_1, agent_2, meaning in new_edges:
        nodes.add(agent_1)
        nodes.add(agent_2)
        key = (agent_1, agent_2, meaning)
        if key in edge_index:
            graph['edges'][edge_index[key]]['weight'] += 1
        else:
            idx = len(graph['edges'])
            graph['edges'].append({
                'agent_1': agent_1,
                'agent_2': agent_2,
                'meaning': meaning,
                'weight': 1
            })
            edge_index[key] = idx

    graph['nodes'] = list(nodes)
    return graph
