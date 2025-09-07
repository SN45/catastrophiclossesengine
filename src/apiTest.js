import axios from "axios";

const api = import.meta.env.VITE_API_URL;

export async function testApi() {
  try {
    const top = await axios.get(`${api}/loss/top?n=10`);
    console.log("Top losses:", top.data);

    const counties = await axios.get(`${api}/loss/counties`);
    console.log("Counties:", counties.data);
  } catch (err) {
    console.error("API error:", err);
  }
}
