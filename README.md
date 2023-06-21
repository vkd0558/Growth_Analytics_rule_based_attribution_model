**Case Study**

We’re working with the Growth analytics team to improve how we attribute user sign ups to our acquisition channels and we want to use custom rules to define which channel should be awarded the credit for a given conversion. We will want to run this attribution model on a daily basis to track our acquisition channel results and we will also continue iterating on the model logic over time. 

**Data Modeling & Approach:** Rule-based attribution model helps in this use case to define the rules and logic for awarding credit to different acquisition channels based on specific criteria (PAID click, Impression, Organic), which includes below. 
 - Identify Attribution Factors – includes first-touch, last-touch time decay consideration
 - Defining Attribution rules – determine how credit should be awarded based on the identified factors
 - Assign credit based on rules – Apply the attribution rules to each conversion or user interaction
 - Aggregate attribution data – calculate the total credit awarded to each acquisition channel over a given rule

**Why Rule-based attribution model:** 
Flexible in defining the attribution rules. we can adjust/add the rules based on your specific business requirements and goals. We can define rules that align with our understanding of user behavior and marketing strategies
Attribution rules: 

 - Paid Click: If a conversion event occurs within 3 hours of a Paid Click session, the Paid Click session will receive 100% attribution credit. It cannot be hijacked by any other sessions
 - Paid Impression: If a conversion event occurs within 1 hour of a Paid Impression session, the Paid Impression session will receive 100% attribution credit. It cannot be hijacked by any other session.
 - Organic Click: If a conversion event occurs within 12 hours of an Organic Click session and there are no intervening Paid Click or Paid Impression sessions, the Organic Click session will receive 100% attribution credit. However, if there is a Paid Click or Paid Impression session within the 12-hour window, the credit will be attributed to the Paid session.
 - Direct: If a user signs up without any live session (Paid or Organic), and the medium is "Direct," the Direct channel will receive 100% attribution credit.
 - Others: If a user signs up without any live session (Paid or Organic) and the medium is not "Direct," the Others channel will receive 100% attribution credit.
 
Refer [Case Study.pdf] for more details on implementation using dbt, SQL, Snowflake (https://github.com/vkd0558/Growth_Analytics_role_based_attribution_model/blob/main/Case%20Study.pdf)
