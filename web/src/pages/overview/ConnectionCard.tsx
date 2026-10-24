import { useState } from "react";
import { useConnection, useDownloadClientProperties } from "@/api/queries";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";

/** How clients connect, and for admins the credentials to do so. */
export function ConnectionCard({ admin }: { admin: boolean }) {
  const connection = useConnection();
  const download = useDownloadClientProperties();
  const [downloadError, setDownloadError] = useState<string | null>(null);
  return (
    <Card>
      <CardHeader>
        <CardTitle>
          <h2>Connection</h2>
        </CardTitle>
        <CardDescription>
          Kafka clients connect over TLS with a username and password.
        </CardDescription>
      </CardHeader>
      <CardContent className="flex flex-col gap-4 text-sm">
        {connection.isError && <p className="text-destructive">{connection.error.message}</p>}
        {connection.data && (
          <dl className="grid grid-cols-1 gap-y-1 sm:grid-cols-[max-content_1fr] sm:gap-x-6 sm:gap-y-2 [&>dd]:min-w-0 [&>dd]:break-words max-sm:[&>dd]:mb-2">
            <dt className="text-muted-foreground">Bootstrap servers</dt>
            <dd>
              <code className="break-all">
                {connection.data.bootstrapAddress ?? "Known once the cluster is ready"}
              </code>
            </dd>
            <dt className="text-muted-foreground">Security protocol</dt>
            <dd>
              <code>{connection.data.securityProtocol}</code>
            </dd>
            <dt className="text-muted-foreground">SASL mechanism</dt>
            <dd>
              <code>{connection.data.saslMechanism}</code>
            </dd>
            <dt className="text-muted-foreground">Username</dt>
            <dd>
              <code>{connection.data.username}</code>
            </dd>
            {connection.data.caCertificate && (
              <>
                <dt className="text-muted-foreground">Certificate</dt>
                <dd>Issued by the cluster&apos;s own CA, which client.properties trusts.</dd>
              </>
            )}
          </dl>
        )}
        {admin ? (
          <div className="flex items-center gap-3">
            <Button
              onClick={() => {
                setDownloadError(null);
                download().catch((error: unknown) => {
                  setDownloadError(error instanceof Error ? error.message : String(error));
                });
              }}
            >
              Download client.properties
            </Button>
            <span className="text-muted-foreground">It holds the password; keep it secret.</span>
          </div>
        ) : (
          <p className="text-muted-foreground">
            Ask an admin of your organization for the password, or for client.properties.
          </p>
        )}
        {downloadError && (
          <p className="text-destructive" role="alert">
            {downloadError}
          </p>
        )}
      </CardContent>
    </Card>
  );
}
