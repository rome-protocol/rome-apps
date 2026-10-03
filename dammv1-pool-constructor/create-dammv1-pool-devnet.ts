
// create-dammv1-pool-devnet.ts
//
// npm install @meteora-ag/dynamic-amm-sdk @solana/web3.js @solana/spl-token bn.js dotenv
//
// Required env:
//   RPC_URL=https://api.devnet.solana.com
//   SECRET_KEY_JSON=[12,34,...]         // wallet secret key bytes
//   TOKEN_A_MINT=...
//   TOKEN_B_MINT=...
//   TOKEN_A_AMOUNT=1000000              // raw units, not UI decimals
//   TOKEN_B_AMOUNT=5000000              // raw units, not UI decimals
//   CONFIG_PUBKEY=...                   // existing DAMM v1 devnet config
//
// Optional:
//   SKIP_PRE_FLIGHT=false

import "dotenv/config";
import AmmImpl, { PROGRAM_ID } from "@meteora-ag/dynamic-amm-sdk";
import { derivePoolAddressWithConfig } from "@meteora-ag/dynamic-amm-sdk/dist/cjs/src/amm/utils";
import {
    Connection,
    Keypair,
    PublicKey,
    sendAndConfirmTransaction,
    Transaction,
} from "@solana/web3.js";
import BN from "bn.js";

function requireEnv(name: string): string {
    const value = process.env[name];
    if (!value) {
        throw new Error(`Missing env var: ${name}`);
    }
    return value;
}

function parseSecretKey(): Keypair {
    const raw = requireEnv("SECRET_KEY_JSON");
    let arr: number[];

    try {
        arr = JSON.parse(raw);
    } catch (e) {
        throw new Error("SECRET_KEY_JSON must be a JSON array of numbers");
    }

    if (!Array.isArray(arr) || arr.some((x) => typeof x !== "number")) {
        throw new Error("SECRET_KEY_JSON must be a JSON array of numbers");
    }

    return Keypair.fromSecretKey(Uint8Array.from(arr));
}

async function main(): Promise<void> {
    const rpcUrl = process.env.RPC_URL ?? "https://api.devnet.solana.com";
    const connection = new Connection(rpcUrl, "confirmed");

    const payer = parseSecretKey();

    const tokenAMint = new PublicKey(requireEnv("TOKEN_A_MINT"));
    const tokenBMint = new PublicKey(requireEnv("TOKEN_B_MINT"));
    const config = new PublicKey(requireEnv("CONFIG_PUBKEY"));

    const tokenAAmount = new BN(requireEnv("TOKEN_A_AMOUNT"));
    const tokenBAmount = new BN(requireEnv("TOKEN_B_AMOUNT"));

    const skipPreflight = (process.env.SKIP_PRE_FLIGHT ?? "false").toLowerCase() === "true";

    const programId = new PublicKey(PROGRAM_ID);
    const poolPubkey = derivePoolAddressWithConfig(tokenAMint, tokenBMint, config, programId);

    console.log("RPC URL:        ", rpcUrl);
    console.log("Payer:          ", payer.publicKey.toBase58());
    console.log("Program ID:     ", programId.toBase58());
    console.log("Token A mint:   ", tokenAMint.toBase58());
    console.log("Token B mint:   ", tokenBMint.toBase58());
    console.log("Config:         ", config.toBase58());
    console.log("Pool PDA:       ", poolPubkey.toBase58());
    console.log("Token A amount: ", tokenAAmount.toString());
    console.log("Token B amount: ", tokenBAmount.toString());

    // The SDK returns Transaction[] for this path.
    const txs: Transaction[] =
        await AmmImpl.createPermissionlessConstantProductPoolWithConfig(
            connection,
            payer.publicKey,
            tokenAMint,
            tokenBMint,
            tokenAAmount,
            tokenBAmount,
            config,
            {
                cluster: "devnet",
                // Optional flags supported by the SDK:
                // lockLiquidity: false,
                // swapLiquidity: { inAmount: new BN(...), minAmountOut: new BN(...) },
                // skipAAta: false,
                // skipBAta: false,
            },
        );

    console.log(`Prepared ${txs.length} transaction(s)`);

    const signatures: string[] = [];

    for (let i = 0; i < txs.length; i++) {
        const tx = txs[i];

        // In case blockhash is stale by the time we send:
        const latest = await connection.getLatestBlockhash("confirmed");
        tx.feePayer = payer.publicKey;
        tx.recentBlockhash = latest.blockhash;

        tx.sign(payer);

        const sig = await sendAndConfirmTransaction(connection, tx, [payer], {
            skipPreflight,
            commitment: "confirmed",
            preflightCommitment: "confirmed",
        });

        signatures.push(sig);
        console.log(`tx[${i}] = ${sig}`);
    }

    console.log("\nDone.");
    console.log("Pool PDA:", poolPubkey.toBase58());
    console.log("Signatures:", signatures);
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});