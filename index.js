const Airtable = require("airtable");
require("dotenv").config();

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID
)(process.env.AIRTABLE_TABLE_ID);

async function main() {
  const topicIDToRecord = {};
  await base
    .select({ view: "Default", fields: ["Link"] })
    .eachPage((records, fetchNextPage) => {
      records.forEach((record) => {
        topicIDToRecord[record.get("Link")] = record.getId();
      });
      fetchNextPage();
    });
  console.log(
    `Fetched ${Object.keys(topicIDToRecord).length} records from airtable.`
  );

  const discourseTopics = {};
  for await (const response = fetch('https://discuss.ray.io/c/dashboard/9.json')
  ) {
    for (const topic of response.data.topic_list.topics) {
      discourseTopics[topic.id.toString()] = {
        fields: {
          Link: "https://discuss.ray.io/t"+"/"+topic.slug+"/"+topic.id,
          Title: topic.title,
          CreatedAt: topic.created_at,
          UpdatedAt: topic.last_posted_at,
        },
      };
    }
  }
  console.log(`Fetched ${Object.keys(discourseTopics).length} topics from Discourse`);

  const airTableNumbers = new Set(Object.keys(topicIDToRecord));
  const recordToAdd = Object.entries(discourseTopics)
    .filter(([number, _]) => !airTableNumbers.has(number))
    .map(([_, record]) => record);
  const recordToUpdate = Object.entries(discourseTopics)
    .filter(([number, _]) => airTableNumbers.has(number))
    .map(([_, record]) => record);

  console.log(`Adding ${recordToAdd.length} records`);
  for (let i = 0; i < recordToAdd.length; i += 10) {
    const chunk = recordToAdd.slice(i, i + 10);
    await base.create(chunk, {
      typecast: true,
    });
  }

  console.log(`Updating ${recordToUpdate.length} records`);
  for (let i = 0; i < recordToUpdate.length; i += 10) {
    const chunk = recordToUpdate.slice(i, i + 10).map((record) => ({
      id: topicIDToRecord[record.fields["Link"].toString()],
      fields: record.fields,
    }));
    await base.replace(chunk, {
      typecast: true,
    });
  }

  console.log("Done!");
}

main().then(console.log).catch(console.error);
