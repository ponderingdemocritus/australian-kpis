use std::{fs, path::Path};

fn repo_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("testing crate lives under crates/testing")
}

#[test]
fn issue_59_public_docs_site_contract_is_wired() {
    let root = repo_root();

    for path in [
        "apps/docs/package.json",
        "apps/docs/index.html",
        "apps/docs/src/main.ts",
        "apps/docs/scripts/verify-docs.mjs",
        "apps/docs/tsconfig.json",
        "apps/docs/vite.config.ts",
        ".github/workflows/docs.yml",
        "docs/public-api-docs.md",
    ] {
        assert!(
            root.join(path).is_file(),
            "issue #59 should provide `{path}`"
        );
    }

    let package = fs::read_to_string(root.join("apps/docs/package.json"))
        .expect("read docs package manifest");
    for expected in [
        "\"name\": \"@au-kpis/docs\"",
        "\"private\": true",
        "\"build\": \"tsc -p tsconfig.json --pretty false && vite build\"",
        "\"test\": \"pnpm run build && node scripts/verify-docs.mjs\"",
        "\"@scalar/api-reference\"",
    ] {
        assert!(
            package.contains(expected),
            "docs package should contain `{expected}`"
        );
    }

    let main =
        fs::read_to_string(root.join("apps/docs/src/main.ts")).expect("read docs app entrypoint");
    for expected in [
        "from '@scalar/api-reference'",
        "@scalar/api-reference/style.css",
        "from '../../../openapi.json'",
        "createApiReference",
        "Australian KPIs API Reference",
    ] {
        assert!(
            main.contains(expected),
            "docs app should render Scalar from committed OpenAPI via `{expected}`"
        );
    }

    let verify = fs::read_to_string(root.join("apps/docs/scripts/verify-docs.mjs"))
        .expect("read docs verification script");
    for expected in [
        "openapi.json",
        "dist/index.html",
        "assets/",
        "/v1/openapi.json",
        "/v1/observations",
    ] {
        assert!(
            verify.contains(expected),
            "docs verification should prove current OpenAPI content rendered via `{expected}`"
        );
    }

    let workflow =
        fs::read_to_string(root.join(".github/workflows/docs.yml")).expect("read docs workflow");
    for expected in [
        "name: Docs",
        "pnpm --filter @au-kpis/docs test",
        "actions/configure-pages",
        "actions/upload-pages-artifact",
        "actions/deploy-pages",
        "apps/docs/dist",
        "DOCS_BASE_PATH: /australian-kpis/",
    ] {
        assert!(
            workflow.contains(expected),
            "docs workflow should publish deterministic static docs via `{expected}`"
        );
    }

    let docs =
        fs::read_to_string(root.join("docs/public-api-docs.md")).expect("read public docs guide");
    for expected in [
        "Generated from `openapi.json`",
        "Scalar",
        "pnpm --filter @au-kpis/docs test",
        "https://ponderingdemocritus.github.io/australian-kpis/",
        "DOCS_BASE_PATH",
    ] {
        assert!(
            docs.contains(expected),
            "public API docs guide should document `{expected}`"
        );
    }
}
