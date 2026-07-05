# Privacy Policy

Last updated: July 6, 2026

This Privacy Policy explains how the AI Product Discovery Copilot and its supporting backend handle data when you use the public GPT and its connected API.

## Who We Are

AI Product Discovery Copilot is a research workflow that helps analyze public customer feedback for product discovery and opportunity analysis.

If you publish this GPT publicly, replace the contact details below with your own:

- Contact email: `amanharpavatdev@gmail.com`
- Maintainer: `Aman Harpavat`

## What This GPT Does

When you use this GPT:

1. You provide a research brief in natural language.
2. The GPT may ask follow-up questions to lock the brief.
3. After your approval, the GPT sends the locked brief to a backend API.
4. The backend collects publicly available feedback from supported sources such as:
   - Google Play reviews
   - Apple App Store reviews
   - Reddit public discussions
5. The backend processes that public feedback and returns structured evidence for report generation.

## Data We Receive From You

The GPT and backend may receive:

- your research prompt
- follow-up clarifications
- locked brief fields such as:
  - product
  - research scope
  - research goal
  - time window
  - included topics
  - excluded topics
  - research questions
  - success criteria
  - optional country

Please do not submit sensitive personal data, secrets, passwords, payment data, or health information in your prompt.

## Public Data We Collect

The backend collects and processes only publicly accessible feedback from supported external sources. This may include:

- review text
- timestamps
- ratings
- public URLs
- publicly visible engagement signals such as likes or comment counts

We use this public data only to generate research evidence, summaries, clusters, metrics, and reports for the requested analysis.

## How We Use Data

We use data to:

- validate and lock the research brief
- run the requested analysis
- collect and process public feedback
- generate evidence artifacts and research outputs
- debug failures and improve reliability

We do not use the GPT workflow to sell your data.

## Storage and Retention

The backend stores temporary run artifacts and logs so that the GPT can retrieve evidence files during analysis. These files may include:

- raw and cleaned feedback extracts
- clusters and summaries
- charts and diagnostics
- processing notes
- run logs

Run artifacts are temporary and are automatically deleted after a retention window configured by the service operator.

Current default retention in this implementation:

- old completed, partial-success, or failed runs are automatically deleted after approximately 24 hours
- active runs are not deleted while still queued or running

## Sharing

Your brief data is shared only with the systems required to operate this workflow:

- OpenAI, through the Custom GPT and Actions platform
- the hosted backend service for this project
- external public platforms only to the extent required to retrieve public feedback

We do not intentionally sell personal data to third parties.

## Limitations and External Sources

This workflow depends on public third-party platforms. Those services may:

- rate limit requests
- block public access
- return incomplete or changing results

As a result, outputs may rely on partial source coverage, and source limitations are disclosed in the generated analysis.

## Security

We take reasonable steps to reduce unnecessary data retention and limit access to known backend artifacts. However, no internet-connected system can be guaranteed to be perfectly secure.

## Your Choices

You can choose not to submit any prompt.

You should avoid including:

- personal secrets
- credentials
- confidential company data
- regulated personal data

If you are the operator of this GPT and backend, you should provide a contact email for privacy or deletion requests.

## Changes to This Policy

We may update this Privacy Policy over time. The latest version should remain available at the public policy URL linked from the GPT.
