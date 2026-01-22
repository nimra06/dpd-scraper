# Setting Up Self-Hosted Runner for Unlimited Runtime

GitHub Actions hosted runners have a **6-hour maximum limit** that cannot be exceeded, even if you set a higher timeout in the workflow file.

To allow the scraper to run until completion (which may take longer than 6 hours), you need to use **self-hosted runners**.

## Quick Setup

### 1. Create a Self-Hosted Runner

1. Go to your repository on GitHub
2. Navigate to **Settings** → **Actions** → **Runners**
3. Click **New self-hosted runner**
4. Select your operating system (Linux, macOS, or Windows)
5. Follow the instructions to download and configure the runner

### 2. Install and Run the Runner

For Linux (recommended for long-running jobs):

```bash
# Create a folder
mkdir actions-runner && cd actions-runner

# Download the latest runner package
curl -o actions-runner-linux-x64-2.311.0.tar.gz -L https://github.com/actions/runner/releases/download/v2.311.0/actions-runner-linux-x64-2.311.0.tar.gz

# Extract the installer
tar xzf ./actions-runner-linux-x64-2.311.0.tar.gz

# Configure the runner (you'll get a token from GitHub)
./config.sh --url https://github.com/YOUR_USERNAME/YOUR_REPO --token YOUR_TOKEN

# Install as a service (runs automatically on boot)
sudo ./svc.sh install
sudo ./svc.sh start
```

### 3. Update Workflow

The workflow is already configured to use `runs-on: self-hosted`. If you want to switch back to hosted runners, change it to `runs-on: ubuntu-latest`.

## Alternative: Use a VPS/Cloud Instance

If you don't have a server, you can:
1. Rent a VPS (DigitalOcean, AWS EC2, etc.)
2. Install the GitHub Actions runner on it
3. Let it run 24/7 to handle your workflows

## Benefits of Self-Hosted Runners

- ✅ **Unlimited runtime** - No 6-hour limit
- ✅ **No usage limits** - Run as many jobs as you want
- ✅ **Full control** - Customize the environment
- ✅ **Cost-effective** - Only pay for the server/VPS

## Security Note

Self-hosted runners have access to your repository secrets. Make sure:
- The server is secure
- Only trusted users have access
- Keep the runner software updated

## Troubleshooting

If the runner doesn't appear in GitHub:
1. Check that the runner service is running: `sudo ./svc.sh status`
2. Check logs: `./run.sh` (for manual testing)
3. Verify network connectivity to GitHub
