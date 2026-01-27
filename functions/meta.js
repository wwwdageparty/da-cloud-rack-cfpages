
const C_SERVICE = "da-cloud-cfd1-rack";
const C_VERSION = "0.0.1";
let G_INSTANCE = "default";

export async function onRequest({ env }: { env: any }) {
  return new Response(
    JSON.stringify(
      {
        service: C_SERVICE,
        version: C_VERSION,
        instance: env.INSTANCEID || G_INSTANCE,
      },
      null,
      2
    ),
    {
      status: 200,
      headers: { "Content-Type": "application/json" },
    }
  );
}
