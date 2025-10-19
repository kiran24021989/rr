# ==============================================
# Auto Git Push + Railway Deploy Script 🚀
# Works with @railway/cli
# ==============================================

$repoPath = "C:\Users\kiran\telegram-bot\telegram-bot"

# --- Change directory ---
Set-Location $repoPath

# --- Git user config ---
git config --global user.name "Kiran"
git config --global user.email "your.email@example.com"

# --- Pull latest changes and rebase ---
Write-Host "`nPulling latest changes from GitHub..."
try {
    git pull origin main --rebase
} catch {
    Write-Host "Rebase conflict detected, keeping local changes..." -ForegroundColor Yellow
    git rebase --abort
    git reset --merge
}

# --- Stage all changes ---
git add .

# --- Commit with timestamped message ---
$timestamp = Get-Date -Format "yyyy-MM-dd_HH-mm-ss"
$commitMessage = "Update bot $timestamp"
if ($(git status --porcelain) -ne "") {
    git commit -m "$commitMessage"
} else {
    Write-Host "No changes to commit."
}

# --- Push to GitHub ---
git push origin main

# --- Deploy to Railway ---
Write-Host "`nDeploying to Railway..."
# The new CLI uses the same 'railway up' command
try {
    railway up
} catch {
    Write-Host "Ensure you are logged in to Railway CLI and your project is linked." -ForegroundColor Red
}

Write-Host "`n✅ Done! Bot updated and deployed on Railway." -ForegroundColor Green
