# Contribution Guide for STL Metro Data API
❤️Thank you so much for wanting to contribute to our project!

This guide provides step-by-step instructions on how to contribute to the STL Metro Data API project. Your work will help this project grow and hopefully allow more people to understand the City better.

## 1. Clone the project
To begin coding on this project, you can either create a Fork of the entire project (Option A) or just create a branch (Option B). For security reasons, we don't allow people to commit directly to the main branch.

- If you are making major changes to the entire project, we recommend creating a fork. On the other hand, if you are making small changes (like to solve an issue), we recommend you create a branch.

### Method A: Fork and Clone the Repo
- To fork the repo, click **Fork** at the top right of the project's repository page.

- Change the name and description if desired, and then hit **Create Fork**.

- Next you should clone the repo so you can program on your device. Open up the terminal on your device and then run:
    ```bash
    git clone <the url of your forked repo>
    ```
    Replace ```<the url of your forked repo>``` with the URL of your forked repo.
    - An easy way to do this is to go to your forked repo in GitHub, hit the green **Code** button at the top right of the screen then hit the **Local** tab. The URL is listed below.

### Option B: Create a branch
Alternatively, you can create a branch if you want to make small changes at a time.

- To create a branch for this project, go to the page of the issue you are working on (see Step 2).

- On the right side of the Issue page, under **Development**, click **Create a branch** for this issue.

- Change the branch name if desired (remember this name!), make sure the Branch source is *main* and that the option *Checkout locally* is selected. Then hit *Create Branch*.

- Next you should clone the repo and then switch to your branch so you can work on it on your own device. In a terminal, run:
    ```bash
    git clone https://github.com/oss-slu/stl_metro_data_api
    git fetch origin
    git checkout <name of your branch>
    ```
    Replace ```<name of your branch>``` with the name of the branch you just created.

- The reason we are asking you to create branches in this way is to allow your branch to be linked to the issue you are working on to allow for seamless integration and progress tracking.


## 2. Explore Good First Issues
- Go to the **Issues** tab of the STL Metro Data API main repository page (https://github.com/oss-slu/stl_metro_data_api/issues).

- Issues that are relatively easy to do are labeled **good first issue**.

- Once you find an issue you like to work on, assign yourself to it.
   - Click on the issue you want to work on.
   - On the right side of the Issue page, under **Assignees**, click **Assign yourself** to let others know you are working on this issue.

## 3. Make Changes (Docs or Code)
For detailed instructions on how to get the code on your device running and what needs to be installed and configured, please see the [Setup Guide](/setup.md).

- The basic outline of our project is as follows:
    - `src`: Contains the `read_service` and `write_service` modules which contains code on retrieving, parsing, saving, and processing data. These modules are written in Python Flask.
    - The `write_service` has two submodules:
        - `processing` for the cleaning and saving of the data
        - `ingestion` for the retrieval of the data
    - `docker` contains all Docker configuration files. Ensure that your new code works in Docker.
    - `docs` contains all documentation files.
    - `tests` contains all tests to ensure the code runs properly. All of the tests except for the `basic_test.py` file is made for `pytest`.
       - Run the tests before submitting your code (more details below) to ensure it works.
       - If you are making new tests, please make sure it works with `pytest`.


- Run tests so you know your code is working right. To do so...
   - In the terminal, go to the project's root directory and run:
      - `pytest tests/`
      - `python tests/basic_test.py`

   - If the tests pass, then your code looks good!

## 4. Submit Pull Request
- Once you feel like you are ready to submit your work, create a pull request. Make sure the tests pass to ensure your code works.
   - Push your changes by running:
       ```bash
       git push origin <name of your branch>
        ```
        Replace ```<name of your branch>``` with the name of your branch.

    - Go to your branch / fork in the GitHub website. There should be a yellow box at the top about recent changes. Click **Create pull request**.

- Put a helpful and descriptive title and description for your pull request.
   - You should link the issue you are working on in your pull request. That way reviewers get to know what issue this PR is related to. It will also automatically close the issue once your PR is merged.
      - Do this by adding `Fixes #27` to your pull request description. Replace `27` with the actual number of the issue you are working on.
   - Your pull request description should also include:
      - What has changed?
      - Why it has changed?
      - How was it changed?
      - Detailed screenshots of the changes.
      - Details of how to run the app and review the changes.

- When you feel like your pull request looks good, then hit **Create pull request** below.

- You should assign a reviewer so someone can review your code. You can do this by hitting the gear icon next to Reviewers on the right side of the Pull Request page. Then select someone to review your code.

## 5. Review & Merge
- Any PR that needs merging to the main needs an approval from a code reviewer with write access (currently only our tech lead Prem has write access).

- If the code reviewer requires changes to your code, please make sure all the review comments are addressed before requesting for a review again.

- Once the code reviewer approves your PR, you are free to merge your code into the main branch.

Congratulations! You have contributed to our project. Thank you so much for your work!