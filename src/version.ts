import packageJson from "../package.json";

/** The package version, sent as the gRPC user-agent so servers can identify the client. */
export const VERSION: string = packageJson.version;

export const USER_AGENT = `sepp-js/${VERSION}`;
