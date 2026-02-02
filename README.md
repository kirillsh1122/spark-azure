# SparkBasics [java]

## Prerequisites

Before proceeding, ensure you have the following tools installed:

- Rancher Desktop – Required for running Kubernetes locally (alternative to Docker Desktop). Please keep it running.
- Maven (mvn) - Required for build Java based app.
- Java – Needed for running Java applications and scripts. Recommended Java version - openjdk 11.
- Azure CLI (az) – Used to interact with Azure services and manage resources.
- Terraform – Infrastructure as Code (IaC) tool for provisioning Azure resources.
- Spark – Unified analytics engine for large-scale data processing; required to run Spark jobs using `spark-submit`.
- dos2unix - command-line tool that converts Windows-style endings (CRLF) to Unix (LF).

📘 Follow the full setup instructions for [Windows environment setup](./setup-windows.md)<br>
🍎 Follow the full setup instructions for [MacOS environment setup](./setup-macos.md)<br>
🐧 Follow the full setup instructions for [Ubuntu 24.10 environment setup](./setup-ubuntu.md)

📌 **Important Guidelines**
Please read the instructions carefully before proceeding. Follow these guidelines to avoid mistakes:

- If you see `<SOME_TEXT_HERE>`, you need to **replace this text and the brackets** with the appropriate value as described in the instructions.
- Follow the steps in order to ensure proper setup.
- Pay attention to **bolded notes**, warnings, or important highlights throughout the document.
- Clean Up Azure Resources Before Proceeding. Since you are using a **free-tier** Azure account, it’s crucial to clean up any leftover resources from previous lessons or deployments before proceeding. Free-tier accounts have strict resource quotas, and exceeding these limits may cause deployment failures.

## 1. Create a Storage Account in Azure for Terraform State

Terraform requires a remote backend to store its state file. Follow these steps:

### **Option 1: Using Azure CLI [Recommended]**

1. **Authenticate with Azure CLI**

Run the following command to authenticate:

```bash
az login
```

💡 **Notes**:
- This will open a browser for authentication.
- If you have **multiple subscriptions**, you will be prompted to **choose one**.
- If you only have **one subscription**, it will be selected by default.
- **Please read the output carefully** to ensure you are using the correct subscription.

2. **Create a Resource Group:**

📌 **Important! Naming Rules for Azure Resources**

<details>
  <summary>👇<strong> Before proceeding, carefully review the naming rules to avoid deployment failures.</strong> 👇 [⬇️⬇️⬇️ Expand to see Naming Rules ⬇️⬇️⬇️]</summary>

### 📝 **Naming Rules for Azure Resources**
Before creating any resources, ensure that you follow **Azure's naming conventions** to avoid errors.

- **Resource names must follow specific character limits and allowed symbols** depending on the resource type.
- **Using unsupported special characters can cause deployment failures.**
- **Storage accounts, resource groups, and other Azure resources have different rules.**

🔹 **Common Rules Across Most Resources**:
- **Allowed characters:** Only **letters (A-Z, a-z)**, **numbers (0-9)**.
- **Case Sensitivity:** Most names are **lowercase only** (e.g., storage accounts).
- **Length Restrictions:** Vary by resource type (e.g., Storage accounts: **3-24 characters**).
- **No special symbols:** Avoid characters like `@`, `#`, `$`, `%`, `&`, `*`, `!`, etc.
- **Hyphens and underscores:** Some resources support them, but rules differ.

📖 **For complete naming rules, refer to the official documentation:**
🔗 [Azure Naming Rules and Restrictions](https://learn.microsoft.com/en-us/azure/azure-resource-manager/management/resource-name-rules)

</details>

To create a Resource Group name run the command:

```bash
az group create --name <RESOURCE_GROUP_NAME> --location <AZURE_REGION>
```

3. **Create a Storage Account:**

⚠️  **Storage Account name, are globally unique, so you must choose a name that no other Azure user has already taken.**

```bash
az storage account create --name <STORAGE_ACCOUNT_NAME> --resource-group <RESOURCE_GROUP_NAME> --location <AZURE_REGION> --sku Standard_LRS
```

4. **Create a Storage Container:**

```bash
az storage container create --name <CONTAINER_NAME> --account-name <STORAGE_ACCOUNT_NAME>
```

### **Option 2: Using Azure Portal (Web UI)**
1. **Log in to [Azure Portal](https://portal.azure.com/)**
2. Navigate to **Resource Groups** and click **Create**.
3. Enter a **Resource Group Name**, select a **Region**, and click **Review + Create**.
4. Once the Resource Group is created, go to **Storage Accounts** and click **Create**.
5. Fill in the required details:
   - **Storage Account Name**
   - **Resource Group** (Select the one you just created)
   - **Region** (Choose your preferred region)
   - **Performance**: Standard
   - **Redundancy**: Locally Redundant Storage (LRS)
6. Click **Review + Create** and then **Create**.
7. Once created, go to the **Storage Account** → **Data Storage** → **Containers** → Click **+ Container**.
8. Name it `tfstate`  (as example) and set **Access Level** to Private.
9. To get `<STORAGE_ACCOUNT_KEY>`: Navigate to your **Storage Account** →**Security & Networking** → **Access Keys**. Press `show` button on `key1`

## 2. Get Your Azure Subscription ID

### **Option 1: Using Azure Portal (Web UI)**

1. **Go to [Azure Portal](https://portal.azure.com/)**
2. Click on **Subscriptions** in the left-hand menu.
3. You will see a list of your subscriptions.
4. Choose the subscription you want to use and copy the **Subscription ID**.

### **Option 2: Using Azure CLI**

Retrieve it using Azure CLI:

```bash
az account show --query id --output tsv
```

## 3. Update Terraform Configuration

Navigate into folder `terraform`. Modify `main.tf` and replace placeholders with your actual values.

- Get a Storage Account Key (`<STORAGE_ACCOUNT_KEY>`):

```bash
az storage account keys list --resource-group <RESOURCE_GROUP_NAME> --account-name <STORAGE_ACCOUNT_NAME> --query "[0].value"
```

- **Edit the backend block in `main.tf`:**

```hcl
  terraform {
    backend "azurerm" {
      resource_group_name  = "<RESOURCE_GROUP_NAME>"
      storage_account_name = "<STORAGE_ACCOUNT_NAME>"
      container_name       = "<CONTAINER_NAME>"
      key                  = "<STORAGE_ACCOUNT_KEY>"
    }
  }
  provider "azurerm" {
    features {}
    subscription_id = "<SUBSCRIPTION_ID>"
  }
```

## 4. Deploy Infrastructure with Terraform

To start the deployment using Terraform scripts, you need to navigate to the `terraform` folder.

```bash
cd terraform/
```

Run the following Terraform commands:

```bash
terraform init
```  

```bash
terraform plan -out terraform.plan
```  

```bash
terraform apply terraform.plan
```  

To see the `<RESOURCE_GROUP_NAME_CREATED_BY_TERRAFORM>` (resource group that was created by terraform), run the command:

```bash
terraform output resource_group_name
```

## 5. Verify Resource Deployment in Azure

After Terraform completes, verify that resources were created:

1. **Go to the [Azure Portal](https://portal.azure.com/)**
2. Navigate to **Resource Groups** → **Find `<RESOURCE_GROUP_NAME_CREATED_BY_TERRAFORM>`**
3. Check that the resources (Storage Account, ACR, etc.) are created.

Alternatively, check via CLI:

```bash
az resource list --resource-group <RESOURCE_GROUP_NAME_CREATED_BY_TERRAFORM> --output table
```

## 6. Configure and Use Azure Container Registry (ACR)

Azure Container Registry (ACR) is used to store container images before deploying them to AKS.

1. Get the `<ACR_NAME>` run the following command:

```bash
terraform output acr_login_server
```

2. Authenticate with ACR.

Change the `<ACR_NAME>` with the output from the step `6.1`

```bash
az acr login --name <ACR_NAME>
```  

## 7. Upload the data files into Azure Conatainers

1. Log in to [Azure Portal](https://portal.azure.com/)
2. Go to your STORAGE account => Data Storage => Containers

- to get actual STORAGE account name, you can run a command from the `terraform` folder:

```bash
terraform output storage_account_name
```

3. Choose the conatiner `data`
4. You should see the upload button
5. Upload the  files here.


## 8. Setup local project

1. Add your code in `src/main/`
2. Add and Run UnitTests into folder `src/tests/`
3. Package your artifacts (new .jar file will be created under `docker/target/`):

```bash
mvn clean package
```

4. Go into folder `docker/`, modify the `Dockerfile` if needed.

5. ⚠️  To build the Docker image, choose the correct command based on your CPU architecture (The first time you run this command, it may take up to 60 minutes.):

    <details>
    <summary><code>Linux</code>, <code>Windows</code>, <code>&lt;Intel-based macOS&gt;</code> (<i>click to expand</i>)</summary>


    ```bash
    dos2unix.exe *.sh
    ```

    then:

    ```bash
    docker build -t <ACR_NAME>/spark-jvm-06:latest .
    ```

    </details>
    <details>
    <summary><code>macOS</code> with <code>M1/M2/M3</code> <code>&lt;ARM-based&gt;</code>  (<i>click to expand</i>)</summary>

    ```bash
    docker build --platform linux/amd64 -t <ACR_NAME>/spark-jvm-06:latest .
    ```

    </details>

6. Local testing (Optional)
- Setup local project before push image to ACR, to make sure if everything goes well:

    <details>
    <summary><code>Linux</code>, <code>Windows</code>, <code>&lt;Intel-based macOS&gt;</code> (<i>click to expand</i>)</summary>

    ```bash
    docker run --rm -it <ACR_NAME>/spark-jvm-06:latest spark-submit --class <YOUR_ACTUAL_MAIN_CLASS> --master "local[*]" --jars local:///opt/<YOUR_JAR_FILE>.jar
    ```

    </details>
    <details>
    <summary><code>macOS</code> with <code>M1/M2/M3</code> <code>&lt;ARM-based&gt;</code>  (<i>click to expand</i>)</summary>

    <strong>Important Note for macOS (M1/M2/M3) Users</strong>

    When building the Docker image with the --platform linux/amd64 flag on macOS with Apple Silicon (ARM-based chips), the
    resulting image will be designed for x86_64 architecture (amd64). This is necessary because the image is meant for deployment
    in a Kubernetes cluster running on amd64 nodes.

    However, this also means that running the image locally using docker run without emulation will likely fail due to an architecture mismatch.
    Solution:
    you can build a separate ARM64 version of the image specifically for local testing using:

    ```bash
    docker build -t <ACR_NAME>/spark-jvm-06:local .
    ```

    Then, run the ARM64 version locally:

    ```bash
    docker run --rm -it <ACR_NAME>/spark-jvm-06:local spark-submit --class <YOUR_ACTUAL_MAIN_CLASS> --master "local[*]" --jars local:///opt/<YOUR_J      AR_FILE>.jar
    ```

    </details>

Warning! (Do not forget to delete output folder, created by spark job after local testing)!

7. Push Docker Image to ACR:

```bash
docker push <ACR_NAME>/spark-jvm-06:latest
```  

8. Verify Image in ACR:

```bash
az acr repository list --name <ACR_NAME> --output table
```  

## 8. Retrieve kubeconfig.yaml and Set It as Default

1. Extract `kubeconfig.yaml` from the directory `/terraform`:

First we need to recieve the current `<AKS_NAME>`,run a command:

```bash
terraform output -raw aks_name
```

Then we need to get `<RESOURCE_GROUP_NAME_CREATED_BY_TERRAFORM>`, run a commands:

```bash
terraform output resource_group_name
```

2. Set `kubeconfig.yaml` as Default for kubectl in Current Terminal Session:

Before run change the placeholders:

```bash
az aks get-credentials --resource-group <RESOURCE_GROUP_NAME_CREATED_BY_TERRAFORM> --name <AKS_NAME>
```

3. Verify Kubernetes Cluster Connectivity:

```bash
kubectl get nodes
```

You should able to see list of AKS nodes. Now you're able to communicate with AKS from commandline.

## 9. Launch Spark app in cluster mode on AKS

- To retrive `https://<k8s-apiserver-host>:<k8s-apiserver-port>` run the foolowing command in the `terraform/` folder:

```bash
terraform output aks_api_server_url
```

- Then procceed with `spark-submit` configuration (before execute this command, update the placeholders):

    <details>
    <summary><code>Linux</code>, <code>MacOS</code>  (<i>click to expand</i>)</summary>

    ```bash
    spark-submit \
        --master k8s://https://<k8s-apiserver-host>:<k8s-apiserver-port> \
        --deploy-mode cluster \
        --name sparkbasics \
        --conf spark.kubernetes.container.image=<ACR_NAME>/spark-jvm-06:latest \
        --conf spark.kubernetes.driver.request.cores=500m \
        --conf spark.kubernetes.driver.request.memory=500m \
        --conf spark.kubernetes.executor.request.memory=500m \
        --conf spark.kubernetes.executor.request.cores=500m \
        --conf spark.kubernetes.namespace=default \
        --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
        --conf spark.kubernetes.driver.pod.name=spark-driver \
        --conf spark.kubernetes.executor.instances=1 \
        --class <YOUR_ACTUAL_MAIN_CLASS> \
        --jars local:///opt/<YOUR_JAR_FILE>.jar
        local:///opt/<YOUR_JAR_FILE>.jar
    ```

    </details>
    <details>
    <summary><code>Windows [powershell]</code>  (<i>click to expand</i>)</summary>

    ```bash
    spark-submit `
        --master k8s://https://<k8s-apiserver-host>:<k8s-apiserver-port> `
        --deploy-mode cluster `
        --name sparkbasics `
        --conf spark.kubernetes.container.image=<ACR_NAME>/spark-jvm-06:latest `
        --conf spark.kubernetes.driver.request.cores=500m `
        --conf spark.kubernetes.driver.request.memory=500m `
        --conf spark.kubernetes.executor.request.memory=500m `
        --conf spark.kubernetes.executor.request.cores=500m `
        --conf spark.kubernetes.namespace=default `
        --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark `
        --conf spark.kubernetes.driver.pod.name=spark-driver `
        --conf spark.kubernetes.executor.instances=1 `
        --class <YOUR_ACTUAL_MAIN_CLASS> `
        --jars local:///opt/<YOUR_JAR_FILE>.jar `
        local:///opt/<YOUR_JAR_FILE>.jar
    ```

    </details>

- once jobs finished, you can verify the output from the spark job:

```bash
kubectl logs spark-driver
```

## 10. Destroy Infrastructure (Required Step)

After completing all steps, **destroy the infrastructure** to clean up all deployed resources.

⚠️ **Warning:** This action is **irreversible**. Running the command below will **delete all infrastructure components** created in previous steps.

To remove all deployed resources, run:

```bash
terraform destroy
```
