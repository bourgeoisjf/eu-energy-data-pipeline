#!/bin/bash
# git_push.sh — automatiza pull, add, commit e push
# Agora ignorando arquivos ocultos do macOS (._*)

# 1️⃣ Vá para a raiz do repositório (garanta que o script está aqui)
echo "📂 Mudando para a raiz do repositório..."
cd "$(dirname "$0")"

# 2️⃣ Atualiza o repositório local
echo "⬇️ Fazendo pull das alterações remotas..."
git pull origin main

# 3️⃣ Remove arquivos indesejados do macOS se existirem
echo "🗑 Removendo arquivos ._ do Git (macOS metadata)..."
git rm --cached $(git ls-files | grep '^._') 2>/dev/null

# 4️⃣ Adiciona arquivos corretos ao commit
echo "➕ Adicionando scripts e CSVs processados ao commit..."
git add ingestion/ processing/ data/processed/

# 5️⃣ Pergunta a mensagem do commit
read -p "✏️ Digite a mensagem do commit: " commit_msg

# 6️⃣ Commit
if [ -z "$commit_msg" ]; then
    commit_msg="Update project files"
fi
echo "💾 Fazendo commit com a mensagem: '$commit_msg'..."
git commit -m "$commit_msg"

# 7️⃣ Push para o GitHub
echo "🚀 Enviando para o GitHub..."
git push origin main

echo "✅ Tudo pronto!"
