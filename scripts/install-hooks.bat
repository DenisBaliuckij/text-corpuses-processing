@echo off
copy hooks\pre-push .git\hooks\pre-push
echo Git hooks installed. Run manually: pytest dags/tests/ -k "not spacy_neural" -q
