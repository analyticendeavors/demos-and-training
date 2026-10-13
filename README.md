# Demos and Training

Code and examples from the Analytic Endeavors demos and training sessions on Power BI and Microsoft Fabric. Each folder is a self-contained example with its own README covering setup and how to run it.

| Folder | What it is |
|---|---|
| [pbi-refresh-and-metadata-framework](pbi-refresh-and-metadata-framework) | The workspace from *Master Power BI Refresh Optimisations and Metadata-Driven Frameworks in Fabric*: a metadata-driven Bronze, Silver and Gold framework, plus notebooks for partition and large model refreshes |
| [metadata-examples](metadata-examples) | Two pipeline-only examples that load the NYC Taxi sample warehouse from a control table, one full load and one incremental using a watermark |

## Using it

Clone the repo, or download the folder you need, then follow the README in that folder. Most examples deploy with the Fabric CLI, so you'll need it installed and signed in with `fab auth login`.

The code is written for teaching, so it's kept simple and leaves out the validation and error handling you'd want in production.

## Contact

Built by Steve Campbell and Reid Havens at [Analytic Endeavors](https://analyticendeavors.com). Questions and issues are welcome on the repo.
