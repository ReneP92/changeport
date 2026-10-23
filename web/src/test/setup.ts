import { GlobalRegistrator } from "@happy-dom/global-registrator";

// A DOM for component tests under bun test. Testing Library is imported by
// the tests themselves, after this has registered the globals.
GlobalRegistrator.register({ url: "http://localhost:8088" });
